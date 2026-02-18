#!/bin/bash

set -euo pipefail

go test ./pkg/... "$@"
