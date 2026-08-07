# simpleswe

`simpleswe` is a Kubernetes-native supervisor for autonomous software-engineering tasks created through Hermes Slack, the CLI, or the terminal UI. It runs each task attempt as an immutable Kubernetes Job, executes OpenCode in a repository-specific worker image, validates and pushes the changes, and creates a Bitbucket or GitHub pull request. When enabled, Hermes reports Slack task results in the originating thread.

```text
Slack
  -> optional Hermes gateway sidecar
  -> simpleswe CLI over Pod localhost
  -> simpleswe controller
  -> Kubernetes Job
  -> OpenCode
  -> repository validation
  -> Git push
  -> forge pull request

CLI / TUI
  -> simpleswe controller
```

A Vaxis terminal UI provides a k9s-style operational view of tasks, attempts, Kubernetes resources, logs, validation results, and pull requests.

## Status

The initial vertical slice supports:

- optional Hermes Slack gateway sidecar;
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
- Hermes Slack bot and app-level tokens when the optional sidecar is enabled;
- Bitbucket or GitHub credentials for each configured repository;
- a webhook signing secret for each configured forge provider;
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

Build the derived Hermes image when Slack is required:

```sh
docker build --target hermes \
  -t ghcr.io/simpleswe/simpleswe-hermes:v0.20.0-simpleswe.1 .
```

The Hermes target contains the SimpleSWE CLI and uses the pinned Hermes Agent
v0.20.0 (`v2026.8.3`) base. Publish the derived image with an immutable
non-`latest` release tag or deploy it by digest.

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

This creates a pinned Kubernetes 1.36 cluster, builds and loads the controller image, installs the Helm chart, and verifies the API through a port-forward. Hermes is explicitly disabled; [`examples/values-kind.yaml`](examples/values-kind.yaml) registers the committed `ptqa/simpleswe` GitHub repository. `make local` creates non-overwriting placeholder Secrets so the controller can start and observe.

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

## Hermes Slack Setup

The SimpleSWE controller has no Slack configuration, runtime, credentials,
API, domain, or schema. Hermes is the only Slack integration and runs as an
optional Helm sidecar when `hermes.enabled: true`. Hermes owns Socket Mode and
must be the only consumer of the Slack app token.

Generate the Slack app manifest with Hermes:

```sh
hermes slack manifest --agent-view --write
```

Apply the generated manifest to the Slack app rather than hand-maintaining a
manifest from this list. The pinned v2026.8.3 manifest enables Socket Mode,
uses the app token connection scope `connections:write`, and includes these bot
scopes:

- `chat:write`, `app_mentions:read`;
- `channels:history`, `channels:read`;
- `groups:history`, `groups:read`;
- `im:history`, `im:read`, `im:write`;
- `mpim:history`, `mpim:read`;
- `users:read`, `files:read`, `files:write`;
- `assistant:write`, `commands`, `reactions:read`.

Subscribe the bot to `message.im`, `message.mpim`, `message.channels`,
`message.groups`, `app_mention`, `app_context_changed`, `app_home_opened`,
`reaction_added`, and `reaction_removed`. The bot token is an `xoxb` token, the
Socket Mode app token is an `xapp` token, and the allowed-user value is a
comma-separated list of Slack Member IDs.

Create the pre-existing Secrets from shell environment inputs. Do not put
these values in Helm values or controller configuration:

```sh
: "${SLACK_BOT_TOKEN:?set SLACK_BOT_TOKEN}"
: "${SLACK_APP_TOKEN:?set SLACK_APP_TOKEN}"
: "${SLACK_ALLOWED_USER_IDS:?set comma-separated Slack Member IDs}"
: "${MODEL_PROVIDER_KEY:?set the model-provider API key}"

kubectl create namespace simpleswe --dry-run=client -o yaml | kubectl apply -f -
kubectl -n simpleswe create secret generic simpleswe-hermes-slack \
  --from-literal=bot-token="$SLACK_BOT_TOKEN" \
  --from-literal=app-token="$SLACK_APP_TOKEN" \
  --from-literal=allowed-user="$SLACK_ALLOWED_USER_IDS" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n simpleswe create secret generic simpleswe-hermes-model \
  --from-literal=api-key="$MODEL_PROVIDER_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
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

Configuration is strict: unknown fields, inline credentials, invalid Secret names, and invalid resource settings fail controller startup. The controller configuration has no Slack section; see [`examples/config.yaml`](examples/config.yaml) and [`examples/values-eks.yaml`](examples/values-eks.yaml) for complete examples.

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

Then install a values file containing image references and repository configuration. To enable Hermes, use the current values shape:

```yaml
hermes:
  enabled: true
  image:
    repository: ghcr.io/simpleswe/simpleswe-hermes
    tag: v0.20.0-simpleswe.1
    digest: ""
    pullPolicy: IfNotPresent
  resources:
    requests: {cpu: 250m, memory: 1Gi}
    limits: {cpu: "1", memory: 2Gi}
  config:
    model: openai/gpt-5.6-luna
  storage:
    storageClass: gp3
    size: 10Gi
    accessModes: [ReadWriteOnce]
  secrets:
    slack:
      name: simpleswe-hermes-slack
      keys: {botToken: bot-token, appToken: app-token, allowedUser: allowed-user}
    modelProvider:
      name: simpleswe-hermes-model
      key: api-key
      env: OPENAI_API_KEY
```

The tag above must be published immutably, or replace it with the derived
image digest. Hermes credentials are SecretKeyRef environment variables only.

```sh
helm upgrade --install simpleswe ./deploy/helm/simpleswe \
  --namespace simpleswe \
  --values path/to/values.yaml
```

For EKS:

```sh
helm upgrade --install simpleswe ./deploy/helm/simpleswe \
  --namespace simpleswe \
  --create-namespace \
  --values examples/values-eks.yaml
```

The chart creates one controller Deployment, private API Service, signed `simpleswe-webhooks` Service, ServiceAccount, namespace Role and RoleBinding, controller PVC, optional Hermes sidecar and `/opt/data` PVC, ConfigMap, and default-deny NetworkPolicies. It references pre-existing Secrets and never creates credential values. Deliberately expose only the signed webhook listener and configure `networkPolicy.webhookIngress`; keep the unauthenticated API private.

The API is unauthenticated and is intended for `kubectl port-forward` access only. Keep the default NetworkPolicy enabled unless another access boundary is in place.

## Hermes Slack Operations

For each Slack request, Hermes preserves the request as the task prompt,
requires a configured repository name, and generates one idempotency key. It
calls the local controller with:

```sh
simpleswe task create \
  --address http://127.0.0.1:8080 \
  --idempotency-key REQUEST_KEY \
  REPOSITORY "ENGINEERING REQUEST"
```

Hermes reports the accepted task ID immediately, then runs the following with
`terminal(background=true, notify_on_complete=true)` so the conversation
remains responsive and completion generates a follow-up event:

```sh
simpleswe task wait --address http://127.0.0.1:8080 TASK_ID
```

`task wait` outputs the final task JSON when a pull-request URL appears or the
task reaches `failed`, `cancelled`, or `ready`. Hermes reports that result to
the originating thread. Retrying `task create` with the same idempotency key
returns the existing task instead of creating a duplicate.

Hermes is limited to `task create`, `list`, `show`, `wait`, `logs`, `cancel`,
and `retry`. It never invokes the SimpleSWE controller, worker, or TUI
top-level commands; it asks for confirmation before cancellation or retry
unless the user explicitly requested that operation.

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

## Architecture

The controller is the only component that owns task intent and logical state. Kubernetes remains the execution scheduler and source of truth for Job and Pod lifecycle.

- The controller has no Slack runtime, credentials, API, domain, or schema. SQLite stores task intent, attempts, events, validation results, Git results, pull requests, log checkpoints, and idempotency records.
- When enabled, Hermes owns Slack Socket Mode and conversations, and calls the SimpleSWE CLI at `http://127.0.0.1:8080`.
- One immutable attempt maps to one deterministic Kubernetes Job.
- The task prompt is delivered in a task-specific read-only Secret.
- The controller retains its database, forge-credential, and webhook mounts. Hermes receives Slack and model credentials only through SecretKeyRef environment variables.
- Hermes has a dedicated PVC mounted at `/opt/data`; it has no controller database, forge, webhook, or repository mounts.
- Hermes' skill and toolset are limited to `terminal`, `skills`, and the seven task commands documented above.
- Workers have no Kubernetes API token and cannot create tasks or retries.
- Only `@@simpleswe:` JSON log lines are interpreted as lifecycle events; all other output remains raw logs.
- Retrying creates a new attempt, Job, task branch, and history entry.
- The controller reconciles persisted intent and labelled Kubernetes resources after restart.
- The direct CLI and TUI paths remain unchanged.

The OpenAPI contract is available at [`api/openapi.yaml`](api/openapi.yaml).

## Destructive Cutover and Rollback

This release does not migrate the old controller database or retain the former
Slack integration. Before deploying this version, stop the old Deployment and
delete the existing controller PVC/database:

```sh
kubectl -n simpleswe scale deployment/simpleswe --replicas=0
kubectl -n simpleswe delete pvc simpleswe-data
```

Then perform one cutover:

1. Update the Slack app with the Hermes-generated manifest.
2. Replace the Slack and model-provider Secrets with the Hermes Secret shape.
3. Deploy the controller and Hermes sidecar together.
4. Verify that only Hermes opens the Slack Socket Mode connection.
5. Submit a Slack task and verify task creation, background `task wait`, pull-request creation, and final thread delivery.

Rollback requires reinstalling the old application version and creating a new
empty database compatible with that version. Task history is not migrated.

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
- Hermes must be the only consumer of the Slack app token.
- Keep the namespace dedicated to SimpleSWE: every Pod container inherits the Pod ServiceAccount token. RBAC remains namespace-scoped; no cluster-scoped permissions are required.
- Pull requests are never merged automatically.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
