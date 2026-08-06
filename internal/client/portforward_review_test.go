package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

func TestShouldFallbackPortForwardDial(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "upgrade failure", err: &httpstream.UpgradeFailureError{Cause: errors.New("upgrade rejected")}, want: true},
		{name: "HTTPS proxy", err: errors.New("proxy: unknown scheme: https"), want: true},
		{name: "other error", err: errors.New("connection refused"), want: false},
		{name: "nil", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldFallbackPortForwardDial(tt.err); got != tt.want {
				t.Fatalf("shouldFallbackPortForwardDial(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestPortForwardStartupBudgetIncludesKubernetesDiscovery(t *testing.T) {
	options := PortForwardOptions{
		KubeContext:    "production",
		Namespace:      "team-a",
		Service:        "controller",
		RemotePort:     80,
		StartupTimeout: 50 * time.Millisecond,
	}
	service := testPortForwardService(options.Namespace, options.Service, map[string]string{"app": "controller"}, corev1.ServicePort{
		Port: 80, TargetPort: intstr.FromInt(8080),
	})
	pod := testReadyPortForwardPod(options.Namespace, "controller-pod", map[string]string{"app": "controller"}, 8080, "http")
	started := make(chan struct{})
	result := make(chan error, 1)

	go func() {
		_, err := startPortForward(context.Background(), options, func(setupCtx, _ context.Context) (forwarder, <-chan struct{}, error) {
			kube := fake.NewSimpleClientset(service, pod)
			kube.PrependReactor("get", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
				close(started)
				<-setupCtx.Done()
				return true, nil, setupCtx.Err()
			})
			_, err := resolvePortForwardTarget(setupCtx, kube, options)
			return nil, nil, err
		})
		result <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("service discovery did not start")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("startup error = %v, want context deadline exceeded", err)
		}
		for _, want := range []string{"production", "team-a/controller", "setup"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("startup error = %q, want %q", err, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("startup did not return after its budget expired")
	}
}

func TestPortForwardStartupWaitsForEligiblePod(t *testing.T) {
	options := PortForwardOptions{
		KubeContext:    "production",
		Namespace:      "team-a",
		Service:        "controller",
		RemotePort:     80,
		StartupTimeout: 2 * time.Second,
	}
	service := testPortForwardService(options.Namespace, options.Service, map[string]string{"app": "controller"}, corev1.ServicePort{
		Port: 80, TargetPort: intstr.FromInt(8080),
	})
	pod := testReadyPortForwardPod(options.Namespace, "controller-pod", map[string]string{"app": "controller"}, 8080, "http")
	kube := fake.NewSimpleClientset(service)
	firstList := make(chan struct{})
	podAvailable := make(chan struct{})
	listCalls := 0
	kube.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		listCalls++
		if listCalls == 1 {
			close(firstList)
			return true, &corev1.PodList{}, nil
		}
		<-podAvailable
		return true, &corev1.PodList{Items: []corev1.Pod{*pod}}, nil
	})
	fakeForwarder := newFakePortForwarder(49156, nil)
	close(fakeForwarder.ready)
	type startupResult struct {
		forward *PortForward
		err     error
	}
	result := make(chan startupResult, 1)
	go func() {
		forward, err := startPortForward(context.Background(), options, func(setupCtx, _ context.Context) (forwarder, <-chan struct{}, error) {
			if _, err := resolvePortForwardTarget(setupCtx, kube, options); err != nil {
				return nil, nil, err
			}
			return fakeForwarder, fakeForwarder.ready, nil
		})
		result <- startupResult{forward: forward, err: err}
	}()

	select {
	case <-firstList:
		close(podAvailable)
	case <-time.After(time.Second):
		t.Fatal("initial pod observation did not occur")
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("startPortForward() error = %v", got.err)
		}
		if got.forward.LocalPort() != 49156 {
			t.Fatalf("LocalPort() = %d, want 49156", got.forward.LocalPort())
		}
		if err := got.forward.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("startup did not proceed after an eligible pod appeared")
	}
}

func TestResolvePortForwardTargetCancellationWhileWaitingForEligiblePod(t *testing.T) {
	options := PortForwardOptions{KubeContext: "production", Namespace: "team-a", Service: "controller", RemotePort: 80}
	service := testPortForwardService(options.Namespace, options.Service, map[string]string{"app": "controller"}, corev1.ServicePort{
		Port: 80, TargetPort: intstr.FromInt(8080),
	})
	pending := testReadyPortForwardPod(options.Namespace, "controller-pod", map[string]string{"app": "controller"}, 8080, "http")
	pending.Status.Phase = corev1.PodPending
	kube := fake.NewSimpleClientset(service)
	listed := make(chan struct{})
	var listedOnce sync.Once
	kube.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		listedOnce.Do(func() { close(listed) })
		return true, &corev1.PodList{Items: []corev1.Pod{*pending}}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := resolvePortForwardTarget(ctx, kube, options)
		result <- err
	}()

	select {
	case <-listed:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("initial pod observation did not occur")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("resolvePortForwardTarget() error = %v, want context canceled", err)
		}
		for _, want := range []string{"production", "team-a/controller", "waiting for a running, ready pod"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error = %q, want %q", err, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("pod wait did not return promptly after cancellation")
	}
}

func TestPortForwardStartupTimeoutCancelsUnderlyingRoundTrip(t *testing.T) {
	blocking := &blockingRoundTripper{started: make(chan struct{}), exited: make(chan struct{})}
	config := &rest.Config{Host: "https://cluster.example", TLSClientConfig: rest.TLSClientConfig{Insecure: true}}
	wrapped := make(chan struct{})
	var wrapOnce sync.Once
	config.Wrap(func(http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			wrapOnce.Do(func() { close(wrapped) })
			return blocking.RoundTrip(request)
		})
	})
	requestURL, err := url.Parse("https://cluster.example/api/v1/namespaces/team-a/pods/controller-pod/portforward")
	if err != nil {
		t.Fatalf("parse request URL: %v", err)
	}
	options := PortForwardOptions{
		KubeContext:    "production",
		Namespace:      "team-a",
		Service:        "controller",
		StartupTimeout: 50 * time.Millisecond,
	}
	result := make(chan error, 1)
	go func() {
		_, err := startPortForward(context.Background(), options, func(_ context.Context, forwardingCtx context.Context) (forwarder, <-chan struct{}, error) {
			return newKubernetesPortForward(forwardingCtx, config, requestURL, options, portForwardTarget{PodName: "controller-pod", Port: 8080})
		})
		result <- err
	}()

	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("transport RoundTrip did not start")
	}
	select {
	case <-wrapped:
	case <-time.After(time.Second):
		t.Fatal("pre-existing transport wrapper was not called")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("startup error = %v, want context deadline exceeded", err)
		}
		for _, want := range []string{"production", "team-a/controller", "transport readiness"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("startup error = %q, want %q", err, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("startup did not return after canceling the transport request")
	}
	select {
	case <-blocking.exited:
	case <-time.After(time.Second):
		t.Fatal("underlying RoundTrip did not exit after startup timeout")
	}
}

func TestReadyPortForwardOutlivesStartupContext(t *testing.T) {
	fakeForwarder := newFakePortForwarder(49154, nil)
	close(fakeForwarder.ready)
	contexts := make(chan [2]context.Context, 1)
	forward, err := startPortForward(context.Background(), PortForwardOptions{StartupTimeout: time.Hour}, func(setup, forwarding context.Context) (forwarder, <-chan struct{}, error) {
		contexts <- [2]context.Context{setup, forwarding}
		return fakeForwarder, fakeForwarder.ready, nil
	})
	if err != nil {
		t.Fatalf("startPortForward() error = %v", err)
	}
	contextPair := <-contexts
	setupCtx, forwardingCtx := contextPair[0], contextPair[1]
	select {
	case <-setupCtx.Done():
	default:
		t.Fatal("startup context was not released after readiness")
	}
	select {
	case <-forwardingCtx.Done():
		t.Fatal("startup completion canceled the forwarding context")
	default:
	}
	select {
	case <-fakeForwarder.stopped:
		t.Fatal("completed startup stopped the established tunnel")
	default:
	}
	if err := forward.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestPortForwardRecordedFailureBeatsReadiness(t *testing.T) {
	wantErr := errors.New("forwarding failed")
	fakeForwarder := newFakePortForwarder(49155, nil)
	forward := &PortForward{
		forwarder: fakeForwarder,
		ctx:       context.Background(),
		ready:     make(chan struct{}),
		done:      make(chan struct{}),
		finished:  true,
		waitErr:   wantErr,
	}
	close(fakeForwarder.ready)

	forward.observeReady(fakeForwarder.ready)

	if err := forward.WaitReady(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("WaitReady() error = %v, want already-recorded failure %v", err, wantErr)
	}
	if got := forward.LocalPort(); got != 0 {
		t.Fatalf("LocalPort() = %d after failed startup, want 0", got)
	}
}

type blockingRoundTripper struct {
	started chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (t *blockingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	t.once.Do(func() { close(t.started) })
	<-request.Context().Done()
	close(t.exited)
	return nil, fmt.Errorf("wait for round trip cancellation: %w", request.Context().Err())
}
