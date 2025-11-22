.PHONY: build run docker

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

run: build
	./bin/api &
	./bin/worker &

docker:
	docker-compose up --build
