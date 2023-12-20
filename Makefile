compile:
	protoc api/v1/*.proto --go_out=.

test:
	go test --race ./...