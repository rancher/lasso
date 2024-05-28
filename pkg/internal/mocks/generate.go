//go:generate mockgen --build_flags=--mod=mod -package mocks -destination ./client.go github.com/rancher/lasso/pkg/client SharedClientFactory

package mocks
