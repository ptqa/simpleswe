package run

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/simpleswe/simpleswe/internal/api"
	"github.com/simpleswe/simpleswe/internal/app"
	"github.com/simpleswe/simpleswe/internal/client"
	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/controller"
	controllerruntime "github.com/simpleswe/simpleswe/internal/controller/runtime"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/forge/bitbucket"
	"github.com/simpleswe/simpleswe/internal/forge/github"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/tui"
	"github.com/simpleswe/simpleswe/internal/worker"
)

const (
	controllerService = "simpleswe"
	controllerPort    = 8080
)

// Dependencies returns the deployable implementations used by cmd/simpleswe.
func Dependencies() app.Dependencies {
	return app.Dependencies{
		RunController: RunController,
		NewWorkspace:  newWorkspace,
		RunWorker:     RunWorker,
		ReportWorker:  worker.Report,
		RunTUI:        runTUI,
		PortForward:   portForward,
		CreateTask: func(ctx context.Context, address string, request client.CreateTaskRequest) (client.Task, error) {
			return client.New(address, nil).CreateTask(ctx, request)
		},
		ListTasks: func(ctx context.Context, address string) (client.TaskList, error) {
			return client.New(address, nil).ListTasks(ctx, client.ListOptions{})
		},
		ShowTask: func(ctx context.Context, address, id string) (client.Task, error) {
			return client.New(address, nil).ShowTask(ctx, id)
		},
		WaitTask: func(ctx context.Context, address, id string) (client.Task, error) {
			return client.New(address, nil).WaitTask(ctx, id)
		},
		CancelTask: func(ctx context.Context, address, id string) (client.Task, error) {
			return client.New(address, nil).CancelTask(ctx, id)
		},
		RetryTask: func(ctx context.Context, address, id string) (client.Task, error) {
			return client.New(address, nil).RetryTask(ctx, id)
		},
		StreamLogs: func(ctx context.Context, address, id string, follow bool, output io.Writer) error {
			return client.New(address, nil).StreamLogs(ctx, id, client.LogOptions{Follow: follow, TailLines: 200}, func(line string) error {
				_, err := fmt.Fprintln(output, line)
				return err
			})
		},
	}
}

func RunWorker(ctx context.Context, manifestPath, workspace string, stdout, _ io.Writer) error {
	roots := []string{"/run/secrets"}
	if configured := os.Getenv("SIMPLESWE_SECRET_PATHS"); configured != "" {
		roots = append(roots, filepath.SplitList(configured)...)
	}
	var secrets []string
	seen := make(map[string]struct{})
	for _, root := range roots {
		values, err := mountedSecrets(root)
		if err != nil {
			return err
		}
		for _, value := range values {
			if _, ok := seen[value]; !ok {
				secrets = append(secrets, value)
				seen[value] = struct{}{}
			}
		}
	}
	return (worker.Runner{ManifestPath: manifestPath, WorkspaceDir: workspace, Output: stdout, Secrets: secrets}).Run(ctx)
}

func newWorkspace() (string, func() error, error) {
	parent, err := os.MkdirTemp("", "simpleswe-worker-")
	if err != nil {
		return "", nil, fmt.Errorf("create worker workspace parent: %w", err)
	}
	return filepath.Join(parent, "workspace"), func() error { return os.RemoveAll(parent) }, nil
}

func runTUI(ctx context.Context, address, kubeContext, namespace string, stdin io.Reader, stdout, stderr io.Writer) error {
	return tui.NewRunner(client.New(address, nil), tui.Options{
		Address: address, KubeContext: kubeContext, Namespace: namespace,
		Stdin: stdin, Stdout: stdout, Stderr: stderr,
	}).Run(ctx)
}

func portForward(ctx context.Context, kubeContext, namespace string) (string, func() error, error) {
	forward, err := client.StartPortForward(ctx, automaticPortForwardOptions(kubeContext, namespace))
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("http://127.0.0.1:%d", forward.LocalPort()), forward.Close, nil
}

func automaticPortForwardOptions(kubeContext, namespace string) client.PortForwardOptions {
	return client.PortForwardOptions{
		KubeContext:    kubeContext,
		Namespace:      namespace,
		Service:        controllerService,
		RemotePort:     controllerPort,
		StartupTimeout: 15 * time.Second,
	}
}

func RunController(ctx context.Context, configPath, databasePath string, stdout, stderr io.Writer) (runErr error) {
	// #nosec G304 -- configPath is the operator-supplied controller config path.
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return fmt.Errorf("open config: %w", err)
	}
	cfg, err := config.Load(cfgFile)
	closeErr := cfgFile.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return fmt.Errorf("close config: %w", closeErr)
	}

	db, err := store.Open(ctx, databasePath)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, db.Close()) }()

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load in-cluster Kubernetes config: %w", err)
	}
	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}

	pullRequests, err := newForgeRouter(cfg, "/run/secrets/bitbucket", "/run/secrets/github")
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	control, err := controller.New(db, kube, cfg, pullRequests)
	if err != nil {
		return err
	}
	backend := controllerruntime.NewBackend(db, control)
	runtime, err := controllerruntime.NewRuntime(kube, db, control, backend, controllerruntime.Options{
		Namespace: cfg.Controller.Namespace, SecretRetention: time.Hour, Logger: logger,
		ProcessForgeEvents: control.ProcessForgeEvents,
	})
	if err != nil {
		return err
	}
	webhooks, err := newWebhookHandler(cfg, db)
	if err != nil {
		return err
	}
	mainHandler := http.NewServeMux()
	mainHandler.Handle("/metrics", metricsHandler{store: db})
	mainHandler.Handle("/", api.NewHandler(backend))
	server := &http.Server{
		Addr: cfg.Controller.ListenAddress, Handler: mainHandler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 1 << 20,
	}
	webhookServer := &http.Server{
		Addr: cfg.Controller.WebhookListenAddress, Handler: webhooks,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 1 << 20,
	}
	logger.InfoContext(ctx, "controller starting", "address", cfg.Controller.ListenAddress, "webhook_address", cfg.Controller.WebhookListenAddress, "namespace", cfg.Controller.Namespace)
	return runControllerComponents(ctx, server, webhookServer, runtime.Run)
}

func runControllerComponents(ctx context.Context, server, webhookServer *http.Server, components ...func(context.Context) error) error {
	componentCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(components)+2)
	var wg sync.WaitGroup
	start := func(name string, run func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := run(componentCtx)
			if componentCtx.Err() != nil && (err == nil || errors.Is(err, context.Canceled)) {
				err = nil
			}
			if err == nil && componentCtx.Err() == nil {
				err = fmt.Errorf("%s stopped unexpectedly", name)
			}
			results <- err
		}()
	}
	start("HTTP server", func(context.Context) error {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	start("webhook server", func(context.Context) error {
		err := webhookServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	for i, component := range components {
		start(fmt.Sprintf("controller component %d", i+1), component)
	}

	var first error
	select {
	case <-ctx.Done():
	case first = <-results:
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	shutdownErr := errors.Join(server.Shutdown(shutdownCtx), webhookServer.Shutdown(shutdownCtx))
	shutdownCancel()
	wg.Wait()
	close(results)
	for err := range results {
		if first == nil && err != nil {
			first = err
		}
	}
	if ctx.Err() != nil && first == nil {
		return nil
	}
	return errors.Join(first, shutdownErr)
}

func readSecret(source config.SecretSource, valueEnv, fileEnv, defaultFile, description string) (string, error) {
	file := source.File
	env := source.Env
	if file == "" && env == "" {
		if named := os.Getenv(fileEnv); named != "" {
			file = named
		} else if _, err := os.Stat(defaultFile); err == nil {
			file = defaultFile
		} else {
			env = valueEnv
		}
	}
	var value string
	if file != "" {
		// #nosec G304,G703 -- file is an explicit trusted config or environment setting.
		data, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("read %s file: %w", description, err)
		}
		if len(data) > 1<<20 {
			return "", fmt.Errorf("%s file is too large", description)
		}
		value = strings.TrimSpace(string(data))
	} else {
		value = strings.TrimSpace(os.Getenv(env))
	}
	if value == "" {
		return "", fmt.Errorf("%s is not configured", description)
	}
	return value, nil
}

func readBitbucketCredential(root, secretName, key string) (string, error) {
	path := filepath.Join(root, secretName, key)
	// #nosec G304 -- secretName is validated as one path component by newForgeRouter.
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read Bitbucket credential %s/%s: %w", secretName, key, err)
	}
	if len(data) > 1<<20 {
		return "", fmt.Errorf("Bitbucket credential %s/%s is too large", secretName, key)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("Bitbucket credential %s/%s is empty", secretName, key)
	}
	return value, nil
}

func readGithubToken(root, secretName string) (string, error) {
	path := filepath.Join(root, secretName, "token")
	// #nosec G304 -- secretName is validated as one path component by newForgeRouter.
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read GitHub credential %s/token: %w", secretName, err)
	}
	if len(data) > 1<<20 {
		return "", fmt.Errorf("GitHub credential %s/token is too large", secretName)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("GitHub credential %s/token is empty", secretName)
	}
	return token, nil
}

type pullRequestClient interface {
	GetPullRequest(context.Context, string, string, int) (forge.PullRequestState, error)
}

type forgeRouter map[forge.Target]pullRequestClient

func newForgeClient(target forge.Target, credentialRoot string) (pullRequestClient, error) {
	if target.Provider == forge.ProviderBitbucket {
		username, err := readBitbucketCredential(credentialRoot, target.CredentialsSecret, "username")
		if err != nil {
			return nil, err
		}
		appPassword, err := readBitbucketCredential(credentialRoot, target.CredentialsSecret, "app-password")
		if err != nil {
			return nil, err
		}
		client, err := bitbucket.NewClient(target.BaseURL, username, appPassword)
		if err != nil {
			return nil, fmt.Errorf("create Bitbucket client for %s/%s: %w", target.Owner, target.Repository, err)
		}
		return client, nil
	}

	token, err := readGithubToken(credentialRoot, target.CredentialsSecret)
	if err != nil {
		return nil, err
	}
	client, err := github.NewClient(target.BaseURL, token)
	if err != nil {
		return nil, fmt.Errorf("create GitHub client for %s/%s: %w", target.Owner, target.Repository, err)
	}
	return client, nil
}

func newForgeRouter(cfg config.Config, bitbucketRoot, githubRoot string) (forgeRouter, error) {
	router := make(forgeRouter, len(cfg.Repositories))
	credentials := make(map[[3]string]string, len(cfg.Repositories))
	for _, repository := range cfg.Repositories {
		bitbucketConfigured := repository.Bitbucket.Workspace != "" || repository.Bitbucket.Repository != "" || repository.Bitbucket.CredentialsSecret != ""
		githubConfigured := repository.GitHub.Owner != "" || repository.GitHub.Repository != "" || repository.GitHub.CredentialsSecret != ""
		if bitbucketConfigured == githubConfigured {
			return nil, fmt.Errorf("repository %q must configure exactly one forge", repository.Name)
		}

		target := forge.Target{
			Provider: forge.ProviderBitbucket, BaseURL: cfg.Bitbucket.BaseURL,
			Owner: repository.Bitbucket.Workspace, Repository: repository.Bitbucket.Repository,
			CredentialsSecret: repository.Bitbucket.CredentialsSecret,
		}
		credentialRoot := bitbucketRoot
		if githubConfigured {
			target = forge.Target{
				Provider: forge.ProviderGitHub, BaseURL: cfg.GitHub.BaseURL,
				Owner: repository.GitHub.Owner, Repository: repository.GitHub.Repository,
				CredentialsSecret: repository.GitHub.CredentialsSecret,
			}
			credentialRoot = githubRoot
		}
		if err := forge.ValidateTarget(target); err != nil {
			return nil, fmt.Errorf("invalid %s route for repository %q: %w", target.Provider, repository.Name, err)
		}
		if filepath.Base(target.CredentialsSecret) != target.CredentialsSecret || target.CredentialsSecret == "." || target.CredentialsSecret == ".." || strings.Contains(target.CredentialsSecret, `\`) {
			return nil, fmt.Errorf("unsafe %s credentials Secret name %q", target.Provider, target.CredentialsSecret)
		}

		key := forgeRoute(target)
		coordinate := [3]string{string(key.Provider), key.Owner, key.Repository}
		if previous, exists := credentials[coordinate]; exists {
			if previous != key.CredentialsSecret {
				return nil, fmt.Errorf("conflicting %s credentials configured for %s/%s", target.Provider, target.Owner, target.Repository)
			}
			continue
		}

		client, err := newForgeClient(target, credentialRoot)
		if err != nil {
			return nil, err
		}
		router[key] = client
		credentials[coordinate] = key.CredentialsSecret
	}
	return router, nil
}

func (r forgeRouter) GetPullRequest(ctx context.Context, target forge.Target, number int) (forge.PullRequestState, error) {
	client, err := r.client(target)
	if err != nil {
		return forge.PullRequestState{}, err
	}
	pullRequest, err := client.GetPullRequest(ctx, target.Owner, target.Repository, number)
	if err != nil {
		return forge.PullRequestState{}, fmt.Errorf("get pull request for %s/%s: %w", target.Owner, target.Repository, err)
	}
	return pullRequest, nil
}

func (r forgeRouter) client(target forge.Target) (pullRequestClient, error) {
	if err := forge.ValidateTarget(target); err != nil {
		permanentErr := forge.MarkPermanent(fmt.Errorf("invalid immutable forge route: %w", err))
		return nil, fmt.Errorf("resolve immutable forge route: %w", permanentErr)
	}
	client := r[forgeRoute(target)]
	if client == nil {
		permanentErr := forge.MarkPermanent(fmt.Errorf(
			"no client configured for immutable forge route provider=%q base_url=%q owner=%q repository=%q credentials_secret_name=%q",
			target.Provider, target.BaseURL, target.Owner, target.Repository, target.CredentialsSecret,
		))
		return nil, fmt.Errorf("resolve immutable forge route: %w", permanentErr)
	}
	return client, nil
}

func forgeRoute(target forge.Target) forge.Target {
	target.Owner = strings.ToLower(target.Owner)
	target.Repository = strings.ToLower(target.Repository)
	return target
}

func mountedSecrets(root string) (_ []string, resultErr error) {
	rootFS, err := os.OpenRoot(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open mounted secret root: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rootFS.Close()) }()
	var values []string
	err = fs.WalkDir(rootFS.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !entry.Type().IsRegular() {
			return nil
		}
		data, err := rootFS.ReadFile(path)
		if err != nil {
			return err
		}
		if len(data) > 1<<20 {
			return fmt.Errorf("mounted secret file is too large: %s", path)
		}
		value := strings.TrimSpace(string(data))
		if value != "" {
			values = append(values, value)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read mounted secrets: %w", err)
	}
	return values, nil
}
