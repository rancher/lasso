# Gotools

This directory contains Go-based tools to use with [go
tool](https://tip.golang.org/doc/modules/managing-dependencies#tools).

Each tool is within its own directory with its own `go.mod` file to avoid
dependency conflicts with lasso itself. Because the tool version is recorded in
a `go.mod`/`go.sum` pair, builds are reproducible and the version is bumped by
the same review process as any other dependency, rather than floating.

## Managing tools

**Using a tool**

```sh
go tool -modfile <path to modfile> <tool>
```

For example, to use setup-envtest:

```sh
go tool -modfile gotools/setup-envtest/go.mod setup-envtest -h
```

**Add a new tool**

From repository root:

```sh
TOOLNAME=<tool name>
mkdir -p gotools/"$TOOLNAME"
go mod init -modfile=gotools/"$TOOLNAME"/go.mod github.com/rancher/lasso/gotools/"$TOOLNAME"
go get -tool -modfile=gotools/"$TOOLNAME"/go.mod <module>@<version>
```

**Update an existing tool**

From repository root:

```sh
TOOLNAME=<tool name>
go get -tool -modfile=gotools/"$TOOLNAME"/go.mod <module>@<new version>
```

Note that `go mod tidy` for these modules must be run from inside the tool's
directory. Running `go mod tidy -modfile=gotools/<tool>/go.mod` from the
repository root makes Go scan every package in lasso against the tool's module
graph, which pulls lasso's entire dependency tree into the tool's `go.mod`.
