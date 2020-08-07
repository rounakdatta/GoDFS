FROM golang:latest

COPY . /app
WORKDIR /app

RUN mkdir -p .dndata
RUN make build
EXPOSE 7000

CMD ["./godfs", "datanode", "--port", "7000", "--data-location", ".dndata/"]