test:
	go test -v ./...

build:
	go build -o godfs main.go

launch:
	make build
	docker build -t datanode -f daemon/datanode/Dockerfile .
	docker build -t namenode -f daemon/namenode/Dockerfile .
	docker build -t client -f daemon/client/Dockerfile .
	docker-compose up --scale datanode=6 --remove-orphans --force-recreate
