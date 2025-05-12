SPEC := openapi.yaml
GEN_DIR := clients
GO_PKG := github.com/example/di-collector/clients/go
PY_PKG := di_client

.PHONY: clients clients/go clients/python lint clean

# Requires `oapi-codegen` (Go) and `openapi-generator-cli` (Python).
# Install:
#   go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest
#   npm i -g @openapitools/openapi-generator-cli

clients: clients/go clients/python

clients/go:
	@mkdir -p $(GEN_DIR)/go
	oapi-codegen -package diclient -generate types,client -o $(GEN_DIR)/go/client.gen.go $(SPEC)

clients/python:
	@mkdir -p $(GEN_DIR)/python
	openapi-generator-cli generate \
		-i $(SPEC) \
		-g python \
		-o $(GEN_DIR)/python \
		--package-name $(PY_PKG)

lint:
	@command -v spectral >/dev/null && spectral lint $(SPEC) || \
		echo "spectral not installed; skipping (npm i -g @stoplight/spectral-cli)"

clean:
	rm -rf $(GEN_DIR)
