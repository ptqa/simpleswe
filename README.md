# simpleswe

`simpleswe` is a Kubernetes-native supervisor for software-engineering tasks. Its CLI and k9s-style terminal UI create, observe, and control tasks that run as immutable Kubernetes Jobs. Each worker executes OpenCode in a repository-specific image, validates and pushes the changes, and creates a Bitbucket or GitHub pull request.

The CLI is also the automation boundary for external agents. Any agent that can run commands can create tasks and observe their results; agent choice, chat, conversation, and orchestration stay outside `simpleswe`.

```text
Human operator / external agent
  -> simpleswe CLI or TUI
  -> simpleswe controller
  -> Kubernetes Job
  -> OpenCode
  -> repository validation
  -> Git push
  -> forge pull request
```

A Vaxis terminal UI provides a k9s-style operational view of tasks, attempts, Kubernetes resources, logs, validation results, and pull requests.

## Status

The initial vertical slice supports:

- SQLite task state in WAL mode;
- one Kubernetes Job and task Secret per immutable attempt;
- controller restart reconciliation;
- live and persisted Pod logs;
- bounded OpenCode validation/fix loops;
- deterministic task branches, commits, and non-force pushes;
- Bitbucket Cloud and GitHub pull-request creation;
- cancellation and retry without rewriting attempt history;
- task creation and operations through the internal HTTP API, CLI, and Vaxis TUI;
- machine-safe task creation and observation for external agents;
- namespace-scoped Helm installation without public ingress.

`simpleswe` is intentionally not a generic workflow engine, CI system, Kubernetes operator, browser dashboard, or autonomous merge service.

## Requirements

- Kubernetes 1.29 or newer;
- Helm 3;
- a default or configured `ReadWriteOnce` StorageClass suitable for SQLite WAL locking;
- Bitbucket or GitHub credentials for each configured repository;
- a webhook signing secret for each configured forge provider;
- a repository-specific worker image containing `simpleswe`, OpenCode, Git, GitHub CLI, SSH, language runtimes, and validation tools;
- Go 1.26.5 when building locally;
- `kubectl` locally for automatic port-forwarding and TUI shell access.

## Build

Build the binary:

```sh
go build -o simpleswe ./cmd/simpleswe
```

Build the controller image:

```sh
docker build --target controller -t ghcr.io/example/simpleswe:0.1.0 .
```

The worker target expects a prebuilt OpenCode executable inside the Docker build context:

```sh
mkdir -p bin
cp /path/to/opencode bin/opencode
docker build \
  --target worker \
  --build-arg OPENCODE_BINARY=bin/opencode \
  -t ghcr.io/example/widget-worker:0.1.0 .
```

Repository-specific worker images should extend this target and add only the runtimes and tools required by that repository.

## Local Kubernetes with kind

With Docker, kind, kubectl, and Helm 3 installed, start or refresh the local controller:

```sh
make local
```

This creates a pinned Kubernetes 1.36 cluster, builds and loads the controller image, installs the Helm chart, and verifies the API through a port-forward. Optional agent sidecars are disabled; [`examples/values-kind.yaml`](examples/values-kind.yaml) registers the committed `ptqa/simpleswe` GitHub repository. `make local` creates non-overwriting placeholder Secrets so the controller can start and observe.

Inspect the controller:

```sh
./simpleswe task list --context kind-simpleswe --namespace simpleswe
./simpleswe tui --context kind-simpleswe --namespace simpleswe
```

The placeholders are only for startup and observation. Before executing tasks, replace or provide real GitHub, webhook, and OpenAI credentials, then build and load a worker image:

```sh
mkdir -p bin
cp /path/to/opencode bin/opencode
docker build --target worker --build-arg OPENCODE_BINARY=bin/opencode -t simpleswe-worker:kind .
kind load docker-image --name simpleswe simpleswe-worker:kind
```

Delete the local cluster when finished:

```sh
make local-down
```

## Repository Credentials

Create separate controller credentials for each Bitbucket repository. The Secret must contain `username` and `app-password`:

```sh
kubectl -n simpleswe create secret generic bitbucket-widget \
  --from-literal=username='automation@example.com' \
  --from-literal=app-password='...'
```

For GitHub, create a repository-scoped fine-grained token. `github.credentials_secret_name` identifies the controller Secret, whose `token` key is used to find and create pull requests. When no separate worker Secret is configured, HTTPS workers also mount this Secret to clone and push:

```sh
kubectl -n simpleswe create secret generic github-widget \
  --from-literal=token='github_pat_...'
```

GitHub review follow-ups use `gh`. Expose the worker token as `GH_TOKEN`, as shown in [`examples/values-kind.yaml`](examples/values-kind.yaml); the local example reuses the repository-scoped `github-simpleswe` Secret rather than duplicating the PAT.

For SSH Git access, create a worker Secret whose private key is named `ssh-privatekey`:

```sh
kubectl -n simpleswe create secret generic widget-git-ssh \
  --from-file=ssh-privatekey="$HOME/.ssh/widget_deploy_key"
```

With GitHub SSH clone URLs, `git.ssh_secret` supplies worker clone/push credentials while `github.credentials_secret_name` still supplies the controller PR token. To separate GitHub HTTPS privileges, set `credentials.secret_name` to a worker-only Secret containing a `token` key. Only that Secret is mounted in the worker at `/run/secrets/repository`; the controller PR Secret does not enter the Job. Git scopes the token helper to the configured clone URL and will not return it for another host or repository.

Use repository-scoped credentials with only the permissions needed to clone, push task branches, read pull requests, and create pull requests. Secret values never belong in Helm values or controller configuration.

## Configuration

Repositories are registered by name. A minimal configuration is:

```yaml
controller:
  listen_address: ":8080"
  webhook_listen_address: ":8081"
  namespace: simpleswe
  deadline: 30m
  max_fix_attempts: 3

worker:
  image: ghcr.io/example/default-worker:0.1.0
  command: opencode
  branch_prefix: simpleswe/
  deadline: 30m

bitbucket:
  base_url: https://api.bitbucket.org
  webhook_secret:
    env: BITBUCKET_WEBHOOK_SECRET

github:
  base_url: https://api.github.com
  webhook_secret:
    env: GITHUB_WEBHOOK_SECRET

repositories:
  widget:
    clone_url: git@bitbucket.org:acme/widget.git
    default_branch: main
    worker:
      image: ghcr.io/example/widget-worker:0.1.0
      resources:
        requests:
          cpu: "2"
          memory: 4Gi
        limits:
          cpu: "8"
          memory: 16Gi
      node_selector:
        workload: agents
    git:
      branch_prefix: simpleswe/
      ssh_secret: widget-git-ssh
    opencode:
      command: [opencode, run]
      config_secret: widget-opencode
    validation:
      max_fix_attempts: 2
      commands:
        - [go, test, ./...]
        - [go, vet, ./...]
    bitbucket:
      workspace: acme
      repository: widget
      credentials_secret_name: bitbucket-widget
```

A public GitHub repository can omit `clone_url` and `default_branch`; they default to its public HTTPS URL and `main`:

```yaml
repositories:
  public-widget:
    worker:
      image: ghcr.io/example/widget-worker:0.1.0
    github:
      owner: octo-org
      repository: widget
      credentials_secret_name: github-widget
```

Configuration is strict: unknown fields, inline credentials, invalid Secret names, and invalid resource settings fail controller startup. Agent integrations are configured separately from the controller; see [`examples/config.yaml`](examples/config.yaml) and [`examples/values-eks.yaml`](examples/values-eks.yaml) for complete examples.

For a standalone controller, set `BITBUCKET_WEBHOOK_SECRET` and `GITHUB_WEBHOOK_SECRET` to the exact secrets for configured providers, then run `./simpleswe controller --config config.yaml --database tasks.db`. Configure Bitbucket to POST to `/v1/webhooks/bitbucket`; GitHub uses `/v1/webhooks/github`.

Validation commands are argv arrays and are executed directly without shell interpolation.

## Install

Create the namespace and provider signing keys first (omit an unconfigured provider):

```sh
kubectl create namespace simpleswe
kubectl -n simpleswe create secret generic simpleswe-webhooks \
  --from-literal=github='github-webhook-secret' \
  --from-literal=bitbucket='bitbucket-webhook-secret'
```

Then install a values file containing image references and repository configuration:

```sh
helm upgrade --install simpleswe ./deploy/helm/simpleswe \
  --namespace simpleswe \
  --values path/to/values.yaml
```

For EKS, the example also enables the optional Hermes agent sidecar:

```sh
helm upgrade --install simpleswe ./deploy/helm/simpleswe \
  --namespace simpleswe \
  --create-namespace \
  --values examples/values-eks.yaml
```

The chart creates one controller Deployment, private API Service, signed `simpleswe-webhooks` Service, ServiceAccount, namespace Role and RoleBinding, controller PVC, ConfigMap, and default-deny NetworkPolicies. It can also deploy the packaged Hermes sidecar and its PVC. The chart references pre-existing Secrets and never creates credential values. Deliberately expose only the signed webhook listener and configure `networkPolicy.webhookIngress`; keep the unauthenticated API private.

The API is unauthenticated and is intended for `kubectl port-forward` access only. Keep the default NetworkPolicy enabled unless another access boundary is in place.

## CLI and TUI

The local commands automatically run `kubectl port-forward` to the `simpleswe` Service using the selected kube context and namespace:

```sh
simpleswe tui --context production --namespace simpleswe

simpleswe task create --context production --namespace simpleswe widget "Fix the failing ClaimService tests"
simpleswe task create --context production --namespace simpleswe --idempotency-key request-123 widget "Fix the failing ClaimService tests"
simpleswe task list --context production --namespace simpleswe
simpleswe task show --context production --namespace simpleswe swe-...
simpleswe task wait --context production --namespace simpleswe swe-...
simpleswe task logs --context production --namespace simpleswe swe-...
simpleswe task cancel --context production --namespace simpleswe swe-...
simpleswe task retry --context production --namespace simpleswe swe-...
```

The create command accepts a configured repository name followed by the task prompt. Quote prompts that contain spaces. `--idempotency-key` is optional and lets machine callers safely retry task creation. `task wait` polls until a pull-request URL appears or the task reaches `failed`, `cancelled`, or `ready`, then writes the final JSON.

Use `--address http://127.0.0.1:8080` to connect to an existing port-forward instead.

TUI keys:

| Key | Action |
| --- | --- |
| `j` / `k`, `↑` / `↓` | Move selection |
| `g` / `G` | Jump to first or last task |
| `n` | Create task |
| `enter` | Task and attempt details |
| `l` | Live logs |
| `e` | Event history |
| `d` | Kubernetes Job details |
| `p` | Kubernetes Pod details |
| `s` | Shell into the running worker with `kubectl exec` |
| `r` | Retry task |
| `ctrl-d` | Cancel task |
| `R` | Refresh |
| `t` | Choose color theme |
| `?` | Help |
| `h`, `q`, `esc` | Back or quit |

## Agent Integration

Any command-capable agent can use the same CLI as a human operator. An integration only needs to preserve the user's request, select a configured repository, and create a stable idempotency key for retries:

```sh
simpleswe task create \
  --address http://127.0.0.1:8080 \
  --idempotency-key REQUEST_KEY \
  REPOSITORY "ENGINEERING REQUEST"
```

The agent can report the accepted task ID immediately and observe completion in a background process:

```sh
simpleswe task wait --address http://127.0.0.1:8080 TASK_ID
```

`task wait` outputs the final task JSON when a pull-request URL appears or the task reaches `failed`, `cancelled`, or `ready`. Retrying `task create` with the same idempotency key returns the existing task instead of creating a duplicate. Agents can also use `task list`, `show`, `logs`, `cancel`, and `retry`; confirmation and presentation are the integrating agent's responsibility.

Use `--address` when the agent already has a network path to the private API. Otherwise, the CLI's `--context` and `--namespace` flags manage a local `kubectl port-forward`.

### Optional Hermes Sidecar

Hermes is the chart's default bundled agent example, and [`examples/values-eks.yaml`](examples/values-eks.yaml) enables it. Base chart values leave it disabled so a controller-only install does not require Slack or model-provider credentials. Hermes is not a SimpleSWE runtime dependency; OpenClaw or another agent can use the same CLI contract without the sidecar.

Build the pinned Hermes image containing the `simpleswe` CLI:

```sh
docker build --target hermes \
  -t ghcr.io/simpleswe/simpleswe-hermes:v0.20.0-simpleswe.1 .
```

Publish that image with an immutable tag or digest. Configure `hermes` values and pre-existing Slack/model-provider Secrets as shown in [`examples/values-eks.yaml`](examples/values-eks.yaml). Generate and apply the Slack app manifest from the matching Hermes release:

```sh
hermes slack manifest --agent-view --write
```

The bundled skill limits Hermes to the seven `simpleswe task` commands above, points it at `http://127.0.0.1:8080`, and keeps repository access, Kubernetes execution, and pull-request ownership in SimpleSWE.

## Architecture

The controller is the only component that owns task intent and logical state. Kubernetes remains the execution scheduler and source of truth for Job and Pod lifecycle.

- The CLI and TUI are the supported user interfaces; the CLI is also the automation contract for external agents.
- The controller has no chat or agent runtime. SQLite stores task intent, attempts, events, validation results, Git results, pull requests, log checkpoints, and idempotency records.
- One immutable attempt maps to one deterministic Kubernetes Job.
- The task prompt is delivered in a task-specific read-only Secret.
- External agents own conversation, authorization, and result presentation. They invoke task commands but do not own task state or Kubernetes resources.
- The optional Hermes sidecar follows this boundary and has no controller database, forge, webhook, or repository mounts.
- Workers have no Kubernetes API token and cannot create tasks or retries.
- Only `@@simpleswe:` JSON log lines are interpreted as lifecycle events; all other output remains raw logs.
- Retrying creates a new attempt, Job, task branch, and history entry.
- The controller reconciles persisted intent and labelled Kubernetes resources after restart.

The OpenAPI contract is available at [`api/openapi.yaml`](api/openapi.yaml).

## Upgrading From Built-in Slack

The current schema does not migrate databases created by releases with the former built-in Slack integration. Before upgrading from one of those releases, stop the old Deployment and delete the existing controller PVC/database:

```sh
kubectl -n simpleswe scale deployment/simpleswe --replicas=0
kubectl -n simpleswe delete pvc simpleswe-data
```

Deploy the new controller with the CLI/TUI only or with the agent integration of your choice. If using the packaged Hermes sidecar, update the Slack app and Secrets for Hermes before enabling it. Rollback requires reinstalling the old application version and creating a new empty database compatible with that version. Task history is not migrated.

## Development

Run the complete local checks:

```sh
pre-commit run --all-files
```

Or run the primary commands directly:

```sh
go test -race -cover ./...
go vet ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run ./...
go run github.com/fe3dback/go-arch-lint@v1.15.0 check --project-path .
go run github.com/daveshanley/vacuum@v0.29.2 lint \
  --no-banner --no-style --no-update-check \
  --ruleset vacuum.ruleset.yaml \
  --fail-severity warn api/openapi.yaml
```

Normal tests use fake Kubernetes clients, fake executables, and local HTTP servers; they do not require real agent, OpenCode, or forge credentials.

## Operating Constraints

- Run exactly one controller replica. SQLite and process-local task serialization do not support active-active controllers.
- Use a block-backed PVC with reliable POSIX file locking. The EKS example uses EBS `gp3`.
- The API has no application authentication. Cluster networking is the security boundary.
- Keep the namespace dedicated to SimpleSWE: every Pod container, including optional agent sidecars, inherits the Pod ServiceAccount token. RBAC remains namespace-scoped; no cluster-scoped permissions are required.
- Pull requests are never merged automatically.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
