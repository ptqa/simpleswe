# simpleswe — Product Decisions

This file is authoritative. Do not modify it unless the user explicitly asks to change PRODUCT.md.

## Product

simpleswe runs OpenCode in Kubernetes from a task created through the CLI or terminal UI and returns a GitHub/Bitbucket pull request. Slack users interact through Hermes Agent, which invokes the simpleswe CLI.

simpleswe owns the resulting pull request and addresses quality-gate failures and review comments. Hermes is a conversational ingress and notification layer; it does not execute software-engineering work or manage Kubernetes resources.

```text
Slack
  → Hermes Agent
  → simpleswe CLI
  → simpleswe controller
  → Kubernetes Job
  → OpenCode
  → validation
  → Git push
  → PR
  → Hermes Agent
  → Slack thread

CLI / TUI
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
* Hermes Agent owns Slack Socket Mode, user authorization, conversations, and thread replies.
* Hermes runs as a sidecar container in the simpleswe controller Pod.
* Hermes invokes the simpleswe CLI against the controller API on localhost.
* The initial Hermes integration uses the existing CLI and does not require MCP or a custom Hermes plugin.
* Tasks can be created through Slack, the CLI, or the terminal UI.
* Slack-originated tasks are created by Hermes through the same CLI contract as other callers.
* Hermes reports the accepted task ID immediately and observes the task until it can report the pull request or terminal failure in the originating Slack thread.
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
* Hermes owns Slack session state, conversational interpretation, and Slack delivery.
* simpleswe remains the source of truth for task and attempt state.
* Workers clone, edit, validate, commit, and push.
* Hermes and workers do not manage Kubernetes resources directly.
* Workers do not communicate with Slack.

## Non-goals

* Browser UI.
* General workflow engine.
* Custom job queue.
* Redis, RabbitMQ, Kafka, Temporal, or Argo Workflows.
* Kubernetes CRDs or custom operators.
* Multi-agent orchestration; Hermes is only Slack ingress and OpenCode remains the code-execution agent.
* Building a custom Slack conversation, memory, authorization, or approval system inside simpleswe.
* Reimplementing Hermes Agent capabilities inside simpleswe.
* A custom Hermes MCP server or native plugin until direct CLI invocation proves insufficient.
* Giving Hermes responsibility for Kubernetes Jobs, repository credentials, validation, Git operations, or pull-request creation.
* Allowing Hermes to modify repositories directly; OpenCode workers remain the execution boundary.
* Multi-tenancy.
* Automatic PR merging.
* AWS Lambda.
* Speculative abstractions for additional agents, forges, or cloud providers.
