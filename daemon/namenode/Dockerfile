FROM golang:latest

COPY . /app
WORKDIR /app

RUN make build
EXPOSE 9000

CMD ["./godfs", "namenode", "--block-size", "10", "--replication-factor", "3"]