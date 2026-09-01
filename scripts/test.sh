#!/bin/bash

set -euo pipefail

go test -race ./pkg/... "$@"
