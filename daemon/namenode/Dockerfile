FROM golang:latest

COPY . /app
WORKDIR /app

RUN make build
EXPOSE 7000

CMD ["./godfs", "namenode", "--datanodes", "localhost:7000,localhost:7001,localhost:7002", "--block-size", "10", "--replication-factor", "2"]