FROM golang:latest

COPY . /app
WORKDIR /app

RUN mkdir -p .dndata
RUN make build

CMD ["./godfs", "datanode", "--data-location", ".dndata/"]