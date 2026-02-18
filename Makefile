all: validate build

validate:
	go fmt ./...
	go vet ./...

build:
	go build ./...
test:
	bash scripts/test.sh
test-integration:
	bash scripts/test-integration.sh

lint:
	golangci-lint run

deps:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2
