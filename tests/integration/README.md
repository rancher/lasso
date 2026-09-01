# Integration Tests

These tests exercise lasso against a real Kubernetes API server and etcd, rather
than a fake client. They use [envtest](https://book.kubebuilder.io/reference/envtest),
which runs a control plane (`kube-apiserver` + `etcd`) as local binaries with no
container runtime or cluster required.

## Running the tests

From the repository root:

```sh
make test-integration
```

That target runs `scripts/test-integration.sh`, which:

1. Runs `scripts/install-envtest.sh` to make sure the pinned control plane
   binaries are present.
2. Exports `KUBEBUILDER_ASSETS` pointing at those binaries.
3. Runs `go test ./tests/integration/...` with coverage against `./pkg/...`.

The unit tests (`make test`) do not need envtest; they only cover `./pkg/...`.

## How the envtest binaries are installed

`scripts/install-envtest.sh` owns two separate pinned things:

- **The `setup-envtest` tool**, pinned in `gotools/setup-envtest/go.mod` and
  invoked as `go tool -modfile gotools/setup-envtest/go.mod setup-envtest`. See
  [gotools/README.md](../../gotools/README.md).
- **The control plane binaries**, pinned as `ENVTEST_VERSION` in the script,
  with a SHA-512 checksum per supported platform.

The script downloads the release tarball from `kubernetes-sigs/controller-tools`,
verifies its checksum, and hands it to `setup-envtest sideload`. If the version
is already in the local envtest store, the download is skipped entirely, so
repeat runs are offline and fast.

It also refuses to run when the envtest minor version does not match the
`k8s.io/client-go` minor version in `go.mod`:

```
k8s.io/client-go minor version (36) does not match envtest minor version (35)
```

## Why these are pinned

Earlier revisions of this suite used `go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest`
and let `setup-envtest` fetch whatever assets it wanted. That was replaced
because:

- **Reproducibility.** With `@latest`, the tool version depends on the day CI
  ran. A green run yesterday and a red run today can differ by a dependency
  nobody changed, which is expensive to debug and impossible to bisect.
- **Supply chain.** `@latest` resolves to whatever the module proxy serves at
  that moment, and the downloaded control plane binaries were not verified at
  all. Both are now pinned and checksummed, so a tampered or swapped artifact
  fails the build instead of executing.
- **Testing the right API server.** envtest ships a `kube-apiserver` matched to
  a Kubernetes minor version. Running lasso's `client-go` against a control
  plane from a different minor tests a combination no user runs, and can pass or
  fail for reasons unrelated to the change under review. Hence the guard above.

This mirrors the approach in [steve](https://github.com/rancher/steve), which
pinned the same tooling for the same reasons.

## Bumping envtest

When `k8s.io/client-go` moves to a new minor version, the envtest binaries have
to move with it. From the repository root:

```sh
./scripts/bump-envtest.sh
```

With no argument this picks the newest published envtest release matching the
current `k8s.io/client-go` minor version. To pin a specific release:

```sh
./scripts/bump-envtest.sh v1.37.0
```

Either way the script rewrites `ENVTEST_VERSION` and every `ENVTEST_SUM_*` line
in `scripts/install-envtest.sh`, reading the checksums straight from the
[upstream release manifest](https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml)
so they are never copied by hand. It refuses to write a version whose minor does
not match `client-go`, and leaves `install-envtest.sh` untouched on any failure.

Afterwards, run `make test-integration` to download and verify the new binaries,
then commit the change to `scripts/install-envtest.sh`.

Note that envtest releases are deliberately **not** managed by Renovate: the
version is a function of `client-go`, not something to bump on its own schedule.

## Adding new tests

Tests live in `tests/integration/` in package `integration`. Each test starts
its own `envtest.Environment` and calls `t.Parallel()`, so every test gets an
isolated control plane and the suite runs concurrently.

Follow the pattern in `sharedcontrollerfactory_test.go`: call `t.Parallel()`,
start the environment, defer `Stop()`, build a lasso factory from the
`*rest.Config` returned by `Start()`, and assert on controller behaviour against
the live API server.

Bear in mind that each parallel test is a full `kube-apiserver` plus `etcd`
process pair, so the suite's memory footprint grows with the number of tests.
