# Generated clients

Run `make clients` from `api/` to regenerate. See `api/Makefile` for the
required generators (`oapi-codegen` and `openapi-generator-cli`).

Generated code is git-ignored — clients are build artifacts of `openapi.yaml`,
not hand-edited sources. Pin generator versions in CI for reproducibility.
