# simpleswe — Product Decisions

This file is authoritative. Do not modify it unless the user explicitly asks to change PRODUCT.md.

## Product

simpleswe runs OpenCode in Kubernetes from tasks created through the CLI or terminal UI and returns a GitHub or Bitbucket pull request. The CLI is also the automation boundary for any external agent that can run commands.

simpleswe owns the resulting pull request and addresses quality-gate failures and review comments.

```text
Human operator / external agent
  → simpleswe CLI or TUI
  → simpleswe controller
  → Kubernetes Job
  → OpenCode
  → validation
  → Git push
  → PR
```

A K9s-style TUI is the primary operational interface for observing, inspecting, and controlling tasks.

## Fixed decisions

* The simpleswe controller, worker, CLI, and TUI are written in Go.
* K9s-style terminal interface built with Vaxis.
* The CLI and terminal UI are the core product interfaces.
* The CLI is the integration contract for external agents.
* Tasks can be created through the CLI, terminal UI, or an external agent invoking the CLI.
* Agent integrations own conversation, user authorization, and result presentation.
* Agent integrations use generic idempotent task creation and task observation commands.
* Hermes is the default bundled Helm example for an agent sidecar, but it is optional.
* Other command-capable agents can use the same CLI contract without SimpleSWE changes.
* When enabled, Hermes runs as a sidecar and invokes the CLI against the controller API on localhost.
* Kubernetes-native controller.
* One Kubernetes Job per task attempt.
* OpenCode first.
* Bitbucket first.
* SQLite in WAL mode on a PVC.
* Single controller replica initially.
* Helm installation.
* Namespace-scoped RBAC.
* The controller Pod ServiceAccount has permissions only in the configured simpleswe namespace.
* The simpleswe namespace is dedicated to simpleswe and contains no unrelated workloads or Secrets.
* No public ingress required.
* Prebuilt repository-specific worker images.
* Retrying creates a new Job and preserves previous attempts.
* The controller owns task state, reconciliation, execution lifecycle, and PR creation.
* simpleswe remains the source of truth for task and attempt state.
* Workers clone, edit, validate, commit, and push.
* External agents and workers do not manage Kubernetes resources directly.
* External agents do not receive repository or forge credentials and do not modify repositories directly.

## Non-goals

* Browser UI.
* General workflow engine.
* Custom job queue.
* Redis, RabbitMQ, Kafka, Temporal, or Argo Workflows.
* Kubernetes CRDs or custom operators.
* Multi-agent orchestration; OpenCode remains the code-execution agent.
* Building chat, conversation, memory, authorization, or approval systems inside simpleswe.
* Requiring or reimplementing a specific external agent framework.
* Agent-specific MCP servers, plugins, or SDKs while direct CLI invocation is sufficient.
* Giving external agents responsibility for Kubernetes Jobs, repository credentials, validation, Git operations, or pull-request creation.
* Allowing external agents to modify repositories directly; OpenCode workers remain the execution boundary.
* Multi-tenancy.
* Automatic PR merging.
* AWS Lambda.
* Speculative abstractions for additional agents, forges, or cloud providers.
