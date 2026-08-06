# simpleswe

`simpleswe` is a Kubernetes-native supervisor for autonomous software-engineering tasks created from Slack, the CLI, or the terminal UI. It runs each task attempt as an immutable Kubernetes Job, executes OpenCode in a repository-specific worker image, validates and pushes the changes, and creates a Bitbucket or GitHub pull request. Slack-originated tasks report the result in the original Slack thread.

```text
Slack / CLI / TUI
  -> simpleswe controller
  -> Kubernetes Job
  -> OpenCode
  -> repository validation
  -> Git push
  -> forge pull request
  -> Slack thread (Slack-originated tasks)
```

A Vaxis terminal UI provides a k9s-style operational view of tasks, attempts, Kubernetes resources, logs, validation results, and pull requests.

## Status

The initial vertical slice supports:

- Slack app mentions and `/simpleswe` commands over Socket Mode;
- durable Slack event deduplication;
- SQLite task state in WAL mode;
- one Kubernetes Job and task Secret per immutable attempt;
- controller restart reconciliation;
- live and persisted Pod logs;
- bounded OpenCode validation/fix loops;
- deterministic task branches, commits, and non-force pushes;
- Bitbucket Cloud and GitHub pull-request creation;
- cancellation and retry without rewriting attempt history;
- task creation and operations through the internal HTTP API, CLI, and Vaxis TUI;
- namespace-scoped Helm installation without public ingress.

`simpleswe` is intentionally not a generic workflow engine, CI system, Kubernetes operator, browser dashboard, or autonomous merge service.

## Requirements

- Kubernetes 1.29 or newer;
- Helm 3;
- a default or configured `ReadWriteOnce` StorageClass suitable for SQLite WAL locking;
- Slack bot and app-level tokens for Socket Mode;
- Bitbucket or GitHub credentials for each configured repository;
- a repository-specific worker image containing `simpleswe`, OpenCode, Git, SSH, language runtimes, and validation tools;
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

Repository-specific worker images should extend this target or reproduce its non-root runtime and add only the runtimes and tools required by that repository.

## Local Kubernetes with kind

With Docker, kind, kubectl, and Helm 3 installed, start or refresh the local controller:

```sh
make local
```

This creates a pinned Kubernetes 1.36 cluster, builds and loads the controller image, installs the Helm chart, and verifies the API through a port-forward. Slack is explicitly disabled and no repositories are registered in [`examples/values-kind.yaml`](examples/values-kind.yaml).

Inspect the controller:

```sh
./simpleswe task list --context kind-simpleswe --namespace simpleswe
./simpleswe tui --context kind-simpleswe --namespace simpleswe
```

The kind values intentionally register no repositories. To execute tasks, build and load a worker image, then add repository configuration and the corresponding Git, OpenCode, and forge Secrets:

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

## Slack Setup

Create a Slack app with:

- Socket Mode enabled;
- an app-level token with `connections:write`;
- bot scopes `app_mentions:read`, `chat:write`, and `commands`;
- an `app_mention` event subscription;
- a `/simpleswe` slash command.

No public request URL is required. Store the tokens in an existing Kubernetes Secret:

```sh
kubectl create namespace simpleswe
kubectl -n simpleswe create secret generic simpleswe-slack \
  --from-literal=bot-token='xoxb-...' \
  --from-literal=app-token='xapp-...'
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
  namespace: simpleswe
  deadline: 30m
  max_fix_attempts: 3

worker:
  image: ghcr.io/example/default-worker:0.1.0
  command: opencode
  branch_prefix: simpleswe/
  deadline: 30m

slack:
  bot_token:
    file: /run/secrets/slack/bot-token
  app_token:
    file: /run/secrets/slack/app-token

bitbucket:
  base_url: https://api.bitbucket.org

github:
  base_url: https://api.github.com

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

Configuration is strict: unknown fields, inline credentials, invalid Secret names, and invalid resource settings fail controller startup. See [`examples/config.yaml`](examples/config.yaml) and [`examples/values-eks.yaml`](examples/values-eks.yaml) for complete examples.

Validation commands are argv arrays and are executed directly without shell interpolation.

## Install

Create a values file containing image references and repository configuration, then install the chart:

```sh
helm upgrade --install simpleswe ./deploy/helm/simpleswe \
  --namespace simpleswe \
  --create-namespace \
  --values path/to/values.yaml
```

For EKS:

```sh
helm upgrade --install simpleswe ./deploy/helm/simpleswe \
  --namespace simpleswe \
  --create-namespace \
  --values examples/values-eks.yaml
```

The chart creates one controller Deployment, ClusterIP Service, ServiceAccount, namespace Role and RoleBinding, PVC, ConfigMap, and default-deny NetworkPolicies. It references pre-existing Secrets and never creates credential values.

The API is unauthenticated and is intended for `kubectl port-forward` access only. Keep the default NetworkPolicy enabled unless another access boundary is in place.

## Slack Commands

```text
@simpleswe run widget Fix the failing ClaimService tests
/simpleswe run widget JIRA-1555
/simpleswe status swe-...
/simpleswe cancel swe-...
/simpleswe retry swe-...
```

Accepted tasks receive a Slack thread with meaningful lifecycle updates and the final pull-request URL. Raw worker logs are not copied into Slack.

## CLI and TUI

The local commands automatically run `kubectl port-forward` to the `simpleswe` Service using the selected kube context and namespace:

```sh
simpleswe tui --context production --namespace simpleswe

simpleswe task create --context production --namespace simpleswe widget "Fix the failing ClaimService tests"
simpleswe task list --context production --namespace simpleswe
simpleswe task show --context production --namespace simpleswe swe-...
simpleswe task logs --context production --namespace simpleswe swe-...
simpleswe task cancel --context production --namespace simpleswe swe-...
simpleswe task retry --context production --namespace simpleswe swe-...
```

The create command accepts a configured repository name followed by the task prompt. Quote prompts that contain spaces.

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

## Architecture

The controller is the only component that owns task intent and logical state. Kubernetes remains the execution scheduler and source of truth for Job and Pod lifecycle.

- SQLite stores task intent, attempts, events, Slack origins, validation results, Git results, pull requests, log checkpoints, and deduplication records.
- One immutable attempt maps to one deterministic Kubernetes Job.
- The task prompt is delivered in a task-specific read-only Secret.
- Long-lived Git, OpenCode, and forge credentials are mounted from separate existing Secrets.
- Workers have no Kubernetes API token and cannot create tasks or retries.
- Only `@@simpleswe:` JSON log lines are interpreted as lifecycle events; all other output remains raw logs.
- Retrying creates a new attempt, Job, task branch, and history entry.
- The controller reconciles persisted intent and labelled Kubernetes resources after restart.

The OpenAPI contract is available at [`api/openapi.yaml`](api/openapi.yaml).

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

Normal tests use fake Kubernetes clients, fake executables, and local HTTP servers; they do not require real Slack, OpenCode, or forge credentials.

## Operating Constraints

- Run exactly one controller replica. SQLite and process-local task serialization do not support active-active controllers.
- Use a block-backed PVC with reliable POSIX file locking. The EKS example uses EBS `gp3`.
- The API has no application authentication. Cluster networking is the security boundary.
- Slack and pull-request notifications are at-least-once around rare crash windows.
- Pull requests are never merged automatically.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
