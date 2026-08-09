# simpleswe — Product Decisions

This file is authoritative. Do not modify it unless the user explicitly asks to change PRODUCT.md.

## Product

simpleswe runs OpenCode in Kubernetes from tasks created through the CLI or terminal UI and returns a GitHub or Bitbucket pull request. The CLI is also the automation boundary for any external agent that can run commands.

OpenCode owns repository and forge actions for the resulting pull request. simpleswe validates and verifies the reported pull request, tracks its lifecycle, and dispatches quality-gate failures and review comments to follow-up OpenCode attempts.

```text
Human operator / external agent
  → simpleswe CLI or TUI
  → simpleswe controller
  → Kubernetes Job
  → OpenCode edits, commits, pushes, and creates or updates the PR
  → OpenCode reports the PR number
  → simpleswe validation and bounded OpenCode repair
  → provider verification
  → PR lifecycle and webhook-driven follow-ups
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
* The controller owns task state, reconciliation, execution lifecycle, reported-PR verification, webhook routing, and follow-up orchestration.
* simpleswe remains the source of truth for task and attempt state.
* OpenCode workers own repository and forge actions: edit, commit, push, PR creation and updates, PR title and body, screenshot evidence, and responses to CI or review feedback.
* The worker runner owns configured validation and emits trusted branch, commit, and reported-PR facts.
* OpenCode reports the PR number through a local simpleswe worker command; a missing or invalid report fails the attempt, and the controller does not create a fallback PR.
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
