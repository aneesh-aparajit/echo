build:
	@mkdir -p ./bin
	@go build -o ./bin/echo

run: build
	@./bin/echo
