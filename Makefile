CLUSTER ?= simpleswe
CONTEXT := kind-$(CLUSTER)
CONTROLLER_IMAGE ?= simpleswe-controller:kind
KIND_NODE_IMAGE ?= kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5

.PHONY: local local-down

local:
	@kind get clusters | grep -qx "$(CLUSTER)" || kind create cluster --name "$(CLUSTER)" --image "$(KIND_NODE_IMAGE)"
	docker build --target controller -t "$(CONTROLLER_IMAGE)" .
	kind load docker-image --name "$(CLUSTER)" "$(CONTROLLER_IMAGE)"
	kubectl --context "$(CONTEXT)" create namespace simpleswe --dry-run=client -o yaml | kubectl --context "$(CONTEXT)" apply -f -
	helm upgrade --install simpleswe ./deploy/helm/simpleswe --kube-context "$(CONTEXT)" --namespace simpleswe --values examples/values-kind.yaml
	kubectl --context "$(CONTEXT)" -n simpleswe rollout restart deployment/simpleswe
	kubectl --context "$(CONTEXT)" -n simpleswe rollout status deployment/simpleswe --timeout=2m
	go build -o simpleswe ./cmd/simpleswe
	./simpleswe task list --context "$(CONTEXT)" --namespace simpleswe

local-down:
	kind delete cluster --name "$(CLUSTER)"
