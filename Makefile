BINARY       := network-reconciler
CMD          := ./cmd/$(BINARY)
DIST         := dist
VERSION      ?= $(shell v=$$(git describe --tags --dirty 2>/dev/null | sed 's/^v//'); echo "$${v:-$$(date -u +0.0.0~dev%Y%m%d%H%M%S)}")
VERSION      := $(VERSION)
ARCH         ?= amd64
HOSTS        ?= root@pve01 root@pve02 root@pve03
LDFLAGS      := -s -w -X main.version=$(VERSION)
NFPM_VERSION ?= v2.39.0
NFPM_BIN     ?= $(CURDIR)/.cache/bin/nfpm

.PHONY: all build package ensure-nfpm deploy test lint clean fmt vet

all: build

## build: compile the binary for linux/$(ARCH).
build:
	@mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=$(ARCH) go build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST)/$(BINARY) $(CMD)
	@echo "Built $(DIST)/$(BINARY) (version=$(VERSION))"

## package: build and package as a .deb.
package: build ensure-nfpm
	@mkdir -p $(DIST)/cfg
	ARCH=$(ARCH) VERSION=$(VERSION) envsubst '$$ARCH $$VERSION' \
		< packaging/nfpm.yaml > $(DIST)/cfg/nfpm-$(ARCH).yaml
	$(NFPM_BIN) package \
		--config $(DIST)/cfg/nfpm-$(ARCH).yaml \
		--packager deb \
		--target $(DIST)/
	@echo "Package written to $(DIST)/"

## deploy: package and ssh-install on HOSTS (space-separated, overridable).
deploy: package
	@deb=$$(ls -t $(DIST)/$(BINARY)_*_$(ARCH).deb 2>/dev/null | head -n1); \
	test -f "$$deb" || { echo "error: no .deb found in $(DIST)/" >&2; exit 1; }; \
	remote_path=/tmp/$$(basename "$$deb"); \
	for host in $(HOSTS); do \
		echo "==> uploading to $$host"; \
		scp -q "$$deb" "$$host:$$remote_path"; \
		echo "==> installing on $$host"; \
		ssh -n "$$host" "set -e; \
			DEBIAN_FRONTEND=noninteractive apt-get install -y \
				--allow-downgrades --reinstall \
				-o Dpkg::Options::=--force-confdef \
				-o Dpkg::Options::=--force-confold \
				'$$remote_path'; \
			systemctl restart $(BINARY); \
			sleep 1; \
			systemctl --no-pager --lines=15 status $(BINARY) || true; \
			rm -f '$$remote_path'"; \
	done

## ensure-nfpm: install nfpm into .cache/bin if not already present.
ensure-nfpm:
	@if [ ! -x "$(NFPM_BIN)" ]; then \
		echo "installing nfpm $(NFPM_VERSION) into $(NFPM_BIN)"; \
		mkdir -p "$(dir $(NFPM_BIN))"; \
		GOBIN="$(dir $(NFPM_BIN))" go install "github.com/goreleaser/nfpm/v2/cmd/nfpm@$(NFPM_VERSION)"; \
	fi

## test: run all unit tests with race detector.
test:
	go test -race -count=1 ./...

## vet: run go vet.
vet:
	go vet ./...

## fmt: format all Go source files.
fmt:
	gofmt -w .

## lint: run golangci-lint (requires golangci-lint in PATH).
lint:
	golangci-lint run ./...

## clean: remove build artefacts.
clean:
	rm -rf $(DIST)

## help: list available targets.
help:
	@grep -E '^## ' Makefile | sed 's/## //'
