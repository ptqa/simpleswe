package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	// Register the official Kubernetes auth plugins used by kubeconfig loading.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

type PortForwardOptions struct {
	KubeContext    string
	Namespace      string
	Service        string
	RemotePort     int
	StartupTimeout time.Duration
}

type portForwardTarget struct {
	PodName string
	Port    int
}

func loadPortForwardConfig(options PortForwardOptions) (*rest.Config, error) {
	overrides := &clientcmd.ConfigOverrides{CurrentContext: options.KubeContext}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), overrides,
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes config for context %q: %w", options.KubeContext, err)
	}
	return config, nil
}

func resolvePortForwardTarget(ctx context.Context, kube kubernetes.Interface, options PortForwardOptions) (portForwardTarget, error) {
	if options.RemotePort < 1 || options.RemotePort > 65535 {
		return portForwardTarget{}, fmt.Errorf("invalid remote port %d: must be between 1 and 65535", options.RemotePort)
	}
	remotePort := int32(options.RemotePort)

	service, err := kube.CoreV1().Services(options.Namespace).Get(ctx, options.Service, metav1.GetOptions{})
	if err != nil {
		return portForwardTarget{}, fmt.Errorf("get service %s/%s: %w", options.Namespace, options.Service, err)
	}
	if len(service.Spec.Selector) == 0 {
		return portForwardTarget{}, fmt.Errorf("service %s/%s has an empty selector", options.Namespace, options.Service)
	}

	var servicePort *corev1.ServicePort
	for i := range service.Spec.Ports {
		if service.Spec.Ports[i].Port == remotePort {
			servicePort = &service.Spec.Ports[i]
			break
		}
	}
	if servicePort == nil {
		return portForwardTarget{}, fmt.Errorf("service %s/%s has no port %d", options.Namespace, options.Service, options.RemotePort)
	}
	if servicePort.Protocol != "" && servicePort.Protocol != corev1.ProtocolTCP {
		return portForwardTarget{}, fmt.Errorf("service %s/%s port %d uses unsupported protocol %s", options.Namespace, options.Service, options.RemotePort, servicePort.Protocol)
	}

	var pod *corev1.Pod
	selector := labels.SelectorFromSet(service.Spec.Selector).String()
	err = wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		pods, err := kube.CoreV1().Pods(options.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return false, fmt.Errorf("list pods for service %s/%s: %w", options.Namespace, options.Service, err)
		}
		sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
		for i := range pods.Items {
			candidate := &pods.Items[i]
			if candidate.Status.Phase == corev1.PodRunning && candidate.DeletionTimestamp == nil && podReady(candidate) {
				pod = candidate
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return portForwardTarget{}, fmt.Errorf("waiting for a running, ready pod for context %q service %s/%s: %w", options.KubeContext, options.Namespace, options.Service, ctxErr)
		}
		return portForwardTarget{}, fmt.Errorf("wait for a running, ready pod for service %s/%s: %w", options.Namespace, options.Service, err)
	}

	if service.Spec.ClusterIP == corev1.ClusterIPNone {
		return portForwardTarget{PodName: pod.Name, Port: int(servicePort.Port)}, nil
	}
	if servicePort.TargetPort.Type == intstr.Int {
		port := servicePort.TargetPort.IntValue()
		if port == 0 {
			port = int(servicePort.Port)
		} else if port < 0 {
			return portForwardTarget{}, fmt.Errorf("service %s/%s has invalid target port %d", options.Namespace, options.Service, port)
		}
		return portForwardTarget{PodName: pod.Name, Port: port}, nil
	}
	name := servicePort.TargetPort.StrVal
	if port, ok := namedContainerPort(pod.Spec.Containers, name); ok {
		return portForwardTarget{PodName: pod.Name, Port: port}, nil
	}
	if port, ok := namedContainerPort(pod.Spec.InitContainers, name); ok {
		return portForwardTarget{PodName: pod.Name, Port: port}, nil
	}
	return portForwardTarget{}, fmt.Errorf("resolve target port %q for service %s/%s on pod %s: no declared container port has that name", name, options.Namespace, options.Service, pod.Name)
}

func namedContainerPort(containers []corev1.Container, name string) (int, bool) {
	for _, container := range containers {
		for _, port := range container.Ports {
			if port.Name == name {
				return int(port.ContainerPort), true
			}
		}
	}
	return 0, false
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

type forwarder interface {
	ForwardPorts() error
	LocalPort() (int, error)
	Close()
}

type spdyForwarder struct {
	*portforward.PortForwarder
	namespace  string
	podName    string
	remotePort int
	cancel     context.CancelFunc
}

func (f *spdyForwarder) ForwardPorts() error {
	if err := f.PortForwarder.ForwardPorts(); err != nil {
		return fmt.Errorf("forward pod %s/%s port %d: %w", f.namespace, f.podName, f.remotePort, err)
	}
	return nil
}

func (f *spdyForwarder) LocalPort() (int, error) {
	ports, err := f.GetPorts()
	if err != nil {
		return 0, fmt.Errorf("get forwarded local port for pod %s/%s: %w", f.namespace, f.podName, err)
	}
	if len(ports) != 1 {
		return 0, fmt.Errorf("expected one forwarded port, got %d", len(ports))
	}
	return int(ports[0].Local), nil
}

func (f *spdyForwarder) Close() {
	f.cancel()
}

func shouldFallbackPortForwardDial(err error) bool {
	return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func portForwardTransportConfig(ctx context.Context, config *rest.Config) *rest.Config {
	forwardingConfig := rest.CopyConfig(config)
	forwardingConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			return next.RoundTrip(request.Clone(ctx))
		})
	})
	return forwardingConfig
}

// PortForward owns an in-process Kubernetes port-forward lifecycle.
type PortForward struct {
	forwarder forwarder
	ctx       context.Context
	ready     chan struct{}
	done      chan struct{}
	stopOnce  sync.Once

	mu        sync.Mutex
	stopping  bool
	finished  bool
	localPort int
	readyErr  error
	waitErr   error
}

// StartPortForward resolves a ready service pod and returns once forwarding is ready.
func StartPortForward(ctx context.Context, options PortForwardOptions) (*PortForward, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, portForwardStartupError(options, "initialization", err)
	}
	return startPortForward(ctx, options, func(setupCtx, forwardingCtx context.Context) (forwarder, <-chan struct{}, error) {
		return setupKubernetesPortForward(setupCtx, forwardingCtx, options)
	})
}

func startPortForward(ctx context.Context, options PortForwardOptions, setup func(context.Context, context.Context) (forwarder, <-chan struct{}, error)) (*PortForward, error) {
	setupCtx, cancelSetup := context.WithCancel(ctx)
	if options.StartupTimeout > 0 {
		setupCtx, cancelSetup = context.WithTimeout(ctx, options.StartupTimeout)
	}
	defer cancelSetup()

	forwarder, ready, err := setup(setupCtx, ctx)
	if err != nil {
		if setupErr := setupCtx.Err(); setupErr != nil && !errors.Is(err, setupErr) {
			err = setupErr
		}
		return nil, portForwardStartupError(options, "setup", err)
	}
	forward := newPortForward(ctx, forwarder, ready)
	if err := forward.WaitReady(setupCtx); err != nil {
		if setupErr := setupCtx.Err(); setupErr != nil {
			err = setupErr
		}
		_ = forward.Close()
		return nil, portForwardStartupError(options, "transport readiness", err)
	}
	return forward, nil
}

func portForwardStartupError(options PortForwardOptions, phase string, err error) error {
	return fmt.Errorf("port-forward startup for context %q service %s/%s during %s: %w", options.KubeContext, options.Namespace, options.Service, phase, err)
}

func setupKubernetesPortForward(setupCtx, forwardingCtx context.Context, options PortForwardOptions) (forwarder, <-chan struct{}, error) {
	config, err := loadPortForwardConfig(options)
	if err != nil {
		return nil, nil, err
	}
	if err := setupCtx.Err(); err != nil {
		return nil, nil, fmt.Errorf("check port-forward setup context before creating Kubernetes client: %w", err)
	}
	kube, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	target, err := resolvePortForwardTarget(setupCtx, kube, options)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve port-forward target: %w", err)
	}

	requestURL := kube.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(options.Namespace).Name(target.PodName).
		SubResource("portforward").URL()
	return newKubernetesPortForward(forwardingCtx, config, requestURL, options, target)
}

func newKubernetesPortForward(ctx context.Context, config *rest.Config, requestURL *url.URL, options PortForwardOptions, target portForwardTarget) (forwarder, <-chan struct{}, error) {
	forwardingCtx, cancel := context.WithCancel(ctx)
	forwardingConfig := portForwardTransportConfig(forwardingCtx, config)
	roundTripper, upgrader, err := spdy.RoundTripperFor(forwardingConfig)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("create port-forward SPDY transport for pod %s/%s: %w", options.Namespace, target.PodName, err)
	}
	spdyDialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, http.MethodPost, requestURL)
	websocketDialer, err := portforward.NewSPDYOverWebsocketDialer(requestURL, forwardingConfig)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("create port-forward WebSocket transport for pod %s/%s: %w", options.Namespace, target.PodName, err)
	}
	ready := make(chan struct{})
	kubeForwarder, err := portforward.NewOnAddresses(
		portforward.NewFallbackDialer(websocketDialer, spdyDialer, shouldFallbackPortForwardDial),
		[]string{"127.0.0.1"},
		[]string{"0:" + strconv.Itoa(target.Port)},
		forwardingCtx.Done(),
		ready,
		io.Discard,
		io.Discard,
	)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("create port-forward for pod %s/%s port %d: %w", options.Namespace, target.PodName, target.Port, err)
	}
	return &spdyForwarder{
		PortForwarder: kubeForwarder,
		namespace:     options.Namespace,
		podName:       target.PodName,
		remotePort:    target.Port,
		cancel:        cancel,
	}, ready, nil
}

func newPortForward(ctx context.Context, forwarder forwarder, forwarderReady <-chan struct{}) *PortForward {
	p := &PortForward{
		forwarder: forwarder,
		ctx:       ctx,
		ready:     make(chan struct{}),
		done:      make(chan struct{}),
	}
	go p.run()
	go p.observeReady(forwarderReady)
	go func() {
		select {
		case <-ctx.Done():
			p.requestStop()
		case <-p.done:
		}
	}()
	return p
}

func (p *PortForward) run() {
	err := p.forwarder.ForwardPorts()
	p.mu.Lock()
	if p.stopping {
		err = nil
	}
	p.waitErr = err
	p.finished = true
	p.mu.Unlock()
	close(p.done)
}

func (p *PortForward) observeReady(forwarderReady <-chan struct{}) {
	var port int
	var err error
	var stop bool
	select {
	case <-forwarderReady:
		port, err = p.forwarder.LocalPort()
		if err != nil {
			err = fmt.Errorf("get bound local port: %w", err)
			stop = true
		}
	case <-p.done:
		p.mu.Lock()
		err = p.waitErr
		stopping := p.stopping
		p.mu.Unlock()
		if err == nil {
			if stopping && p.ctx.Err() != nil {
				err = p.ctx.Err()
			} else {
				err = errors.New("port forwarding stopped before readiness")
			}
		}
	}
	p.markReady(port, err)
	if stop {
		p.requestStop()
	}
}

func (p *PortForward) markReady(port int, err error) {
	p.mu.Lock()
	if p.finished && (err == nil || p.waitErr != nil) {
		port = 0
		err = p.waitErr
		if err == nil {
			err = errors.New("port forwarding stopped during startup")
		}
	}
	p.localPort = port
	p.readyErr = err
	p.mu.Unlock()
	close(p.ready)
}

// WaitReady waits for readiness and returns an early forwarding failure.
func (p *PortForward) WaitReady(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	select {
	case <-p.ready:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.readyErr
	case <-ctx.Done():
		return fmt.Errorf("wait for port-forward readiness: %w", ctx.Err())
	}
}

// LocalPort returns the actual bound local port after readiness.
func (p *PortForward) LocalPort() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.localPort
}

// Wait returns the forwarding result.
func (p *PortForward) Wait() error {
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

// Close stops forwarding and waits for it to exit.
func (p *PortForward) Close() error {
	p.requestStop()
	return p.Wait()
}

func (p *PortForward) requestStop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		if p.finished {
			p.mu.Unlock()
			return
		}
		p.stopping = true
		p.mu.Unlock()
		p.forwarder.Close()
	})
}
