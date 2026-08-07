#!/bin/bash

set -euo pipefail

go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

export PATH=$PATH:$(go env GOPATH)/bin

if [ -z "${KUBEBUILDER_ASSETS:-}" ]; then
    KUBEBUILDER_ASSETS=$(setup-envtest use -p path --bin-dir $(pwd)/testbin 1.35.0)
fi
export KUBEBUILDER_ASSETS

go test -coverpkg ./pkg/... -coverprofile coverage-integration.out ./tests/integration/... "$@"
