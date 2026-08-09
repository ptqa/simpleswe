CLUSTER ?= simpleswe
CONTEXT := kind-$(CLUSTER)
CONTROLLER_IMAGE ?= simpleswe-controller:kind
KIND_NODE_IMAGE ?= kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5

.PHONY: build local local-down

build:
	go build -o simpleswe ./cmd/simpleswe

local:
	@kind get clusters | grep -qx "$(CLUSTER)" || kind create cluster --name "$(CLUSTER)" --image "$(KIND_NODE_IMAGE)"
	docker build --target controller -t "$(CONTROLLER_IMAGE)" .
	kind load docker-image --name "$(CLUSTER)" "$(CONTROLLER_IMAGE)"
	kubectl --context "$(CONTEXT)" create namespace simpleswe --dry-run=client -o yaml | kubectl --context "$(CONTEXT)" apply -f -
	@kubectl --context "$(CONTEXT)" -n simpleswe get secret github-simpleswe >/dev/null 2>&1 || kubectl --context "$(CONTEXT)" -n simpleswe create secret generic github-simpleswe --from-literal=token=placeholder
	@if test -f .secrets/github-simpleswe/token; then \
		TOKEN=$$(kubectl --context "$(CONTEXT)" -n simpleswe create secret generic github-simpleswe --from-file=token=.secrets/github-simpleswe/token --dry-run=client -o jsonpath='{.data.token}'); \
		kubectl --context "$(CONTEXT)" -n simpleswe patch secret github-simpleswe --type=merge -p "{\"data\":{\"token\":\"$$TOKEN\"}}"; \
	else \
		echo "warning: .secrets/github-simpleswe/token missing, preserving existing Secret key"; \
	fi
	@kubectl --context "$(CONTEXT)" -n simpleswe get secret simpleswe-webhooks >/dev/null 2>&1 || kubectl --context "$(CONTEXT)" -n simpleswe create secret generic simpleswe-webhooks --from-literal=github=placeholder --from-literal=bitbucket=placeholder
	@if test -f .secrets/github-simpleswe/webhook; then \
		GITHUB_FILE=.secrets/github-simpleswe/webhook; \
	elif test -f .secrets/github/webhook; then \
		GITHUB_FILE=.secrets/github/webhook; \
	else \
		GITHUB_FILE=""; \
	fi; \
	if test -n "$$GITHUB_FILE"; then \
		GITHUB_WEBHOOK=$$(kubectl --context "$(CONTEXT)" -n simpleswe create secret generic simpleswe-webhooks --from-file=github=$$GITHUB_FILE --dry-run=client -o jsonpath='{.data.github}'); \
		kubectl --context "$(CONTEXT)" -n simpleswe patch secret simpleswe-webhooks --type=merge -p "{\"data\":{\"github\":\"$$GITHUB_WEBHOOK\"}}"; \
	else \
		echo "warning: .secrets github webhook missing, preserving existing Secret key"; \
	fi
	@if test -f .secrets/bitbucket/webhook; then \
		BITBUCKET_FILE=.secrets/bitbucket/webhook; \
	elif test -f .secrets/bitbucket-simpleswe/webhook; then \
		BITBUCKET_FILE=.secrets/bitbucket-simpleswe/webhook; \
	else \
		BITBUCKET_FILE=""; \
	fi; \
	if test -n "$$BITBUCKET_FILE"; then \
		BITBUCKET_WEBHOOK=$$(kubectl --context "$(CONTEXT)" -n simpleswe create secret generic simpleswe-webhooks --from-file=bitbucket=$$BITBUCKET_FILE --dry-run=client -o jsonpath='{.data.bitbucket}'); \
		kubectl --context "$(CONTEXT)" -n simpleswe patch secret simpleswe-webhooks --type=merge -p "{\"data\":{\"bitbucket\":\"$$BITBUCKET_WEBHOOK\"}}"; \
	else \
		echo "warning: .secrets/bitbucket/webhook missing, preserving existing Secret key"; \
	fi
	helm upgrade --install simpleswe ./deploy/helm/simpleswe --kube-context "$(CONTEXT)" --namespace simpleswe --values examples/values-kind.yaml
	kubectl --context "$(CONTEXT)" -n simpleswe rollout restart deployment/simpleswe
	kubectl --context "$(CONTEXT)" -n simpleswe rollout status deployment/simpleswe --timeout=2m
	go build -o simpleswe ./cmd/simpleswe
	./simpleswe task list --context "$(CONTEXT)" --namespace simpleswe

local-down:
	kind delete cluster --name "$(CLUSTER)"
