fmt:
	gofmt -w -s .
test:
	go test --race 


compile:
	protoc *.proto \
			--go_out=. \
			--go-grpc_out=. \
			--go_opt=paths=source_relative \
			--go-grpc_opt=paths=source_relative \
			--proto_path=.
