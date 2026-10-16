COMPOSE := docker compose -f "$(CURDIR)/build/compose.yaml"

BIN_PATH := $(CURDIR)/build/bin

GOLANGCI_LINT_VERSION := v2.14.0
PROTOC_GEN_GO_VERSION := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

.PHONY: generate
generate: generate/proto

.PHONY: generate/proto
generate/proto: bin/protoc-gen-go bin/protoc-gen-go-grpc
	@mkdir -p internal/pkg/gen
	@PATH="$(BIN_PATH):$$PATH" protoc -I proto --go_out=internal/pkg/gen --go_opt=paths=source_relative --go-grpc_out=internal/pkg/gen --go-grpc_opt=paths=source_relative proto/callrelay/v1/*.proto

.PHONY: build
build: | $(BIN_PATH)
	CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o "$(BIN_PATH)/callrelay" ./cmd/callrelay

.PHONY: vet
vet:
	@go vet ./...

.PHONY: lint
lint: bin/golangci-lint
	@"$(BIN_PATH)/golangci-lint" run --config build/ci/.golangci.yml

.PHONY: fmt
fmt: bin/golangci-lint
	@"$(BIN_PATH)/golangci-lint" fmt --config build/ci/.golangci.yml
	@"$(BIN_PATH)/golangci-lint" run --config build/ci/.golangci.yml --enable-only wsl_v5 --fix

.PHONY: infra/up
infra/up:
	@$(COMPOSE) up -d --wait

.PHONY: infra/down
infra/down:
	@$(COMPOSE) down

$(BIN_PATH):
	@mkdir -p "$@"

.PHONY: bin/golangci-lint
bin/golangci-lint: $(BIN_PATH)/golangci-lint

$(BIN_PATH)/golangci-lint: | $(BIN_PATH)
	@curl -fsSL "https://raw.githubusercontent.com/golangci/golangci-lint/$(GOLANGCI_LINT_VERSION)/install.sh" -o "$(BIN_PATH)/.golangci-lint-install.sh"
	@sh "$(BIN_PATH)/.golangci-lint-install.sh" -b "$(BIN_PATH)" "$(GOLANGCI_LINT_VERSION)"
	@rm "$(BIN_PATH)/.golangci-lint-install.sh"

.PHONY: bin/protoc-gen-go
bin/protoc-gen-go: $(BIN_PATH)/protoc-gen-go

$(BIN_PATH)/protoc-gen-go: | $(BIN_PATH)
	@GOBIN="$(BIN_PATH)" go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

.PHONY: bin/protoc-gen-go-grpc
bin/protoc-gen-go-grpc: $(BIN_PATH)/protoc-gen-go-grpc

$(BIN_PATH)/protoc-gen-go-grpc: | $(BIN_PATH)
	@GOBIN="$(BIN_PATH)" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
