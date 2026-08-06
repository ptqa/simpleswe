# simpleswe — Product Decisions

This file is authoritative. Do not modify it unless the user explicitly asks to change PRODUCT.md.

## Product

simpleswe runs OpenCode in Kubernetes from a task created through Slack, the CLI, or the terminal UI and returns a GitHub/Bitbucket pull request.

```text
Slack / CLI / TUI
  → simpleswe controller
  → Kubernetes Job
  → OpenCode
  → validation
  → Git push
  → PR
  → Slack thread (Slack-originated tasks)
```

A K9s-style TUI is the primary operational interface for observing, inspecting, and controlling tasks.

## Fixed decisions

* Go.
* K9s-style terminal interface built with Vaxis.
* Slack Socket Mode.
* Tasks can be created through Slack, the CLI, or the terminal UI.
* Kubernetes-native controller.
* One Kubernetes Job per task attempt.
* OpenCode first.
* Bitbucket first.
* SQLite in WAL mode on a PVC.
* Single controller replica initially.
* Helm installation.
* Namespace-scoped RBAC.
* No public ingress required.
* Prebuilt repository-specific worker images.
* Retrying creates a new Job and preserves previous attempts.
* The controller owns state, reconciliation, Slack updates, and PR creation.
* Workers clone, edit, validate, commit, and push.
* Workers do not manage Kubernetes resources or communicate with Slack.

## Non-goals

* Browser UI.
* General workflow engine.
* Custom job queue.
* Redis, RabbitMQ, Kafka, Temporal, or Argo Workflows.
* Kubernetes CRDs or custom operators.
* Multi-agent orchestration.
* Multi-tenancy.
* Automatic PR merging.
* AWS Lambda.
* Speculative abstractions for agents, forges, or cloud providers.
