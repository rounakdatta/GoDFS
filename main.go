package main

import (
	"flag"
	"github.com/rounakdatta/GoDFS/daemon"
	"log"
	"os"
	"strings"
)

func main() {
	dataNodeCommand := flag.NewFlagSet("datanode", flag.ExitOnError)
	nameNodeCommand := flag.NewFlagSet("namenode", flag.ExitOnError)
	clientCommand := flag.NewFlagSet("client", flag.ExitOnError)

	dataNodePortPtr := dataNodeCommand.Int("port", 7000, "DataNode communication port")
	dataNodeDataLocationPtr := dataNodeCommand.String("data-location", ".", "DataNode data storage location")

	nameNodePortPtr := nameNodeCommand.Int("port", 9000, "NameNode communication port")
	nameNodeListPtr := nameNodeCommand.String("list-datanodes", "", "Comma-separated list of DataNodes to connect to")

	clientNameNodePortPtr := clientCommand.Int("namenode-address", 9000, "NameNode communication port")
	clientPortPtr := clientCommand.Int("port", 7000, "Client communication port")

	if len(os.Args) < 2 {
		log.Println("sub-command is required")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "datanode":
		_ = dataNodeCommand.Parse(os.Args[2:])
		daemon.InitializeDataNodeUtil(*dataNodePortPtr, *dataNodeDataLocationPtr)
	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		listOfDataNodes := strings.Split(*nameNodeListPtr, ",")
		daemon.InitializeNameNodeUtil(*nameNodePortPtr, listOfDataNodes)
	case "client":
		_ = clientCommand.Parse(os.Args[2:])
		daemon.InitializeClientUtil(*clientPortPtr, *clientNameNodePortPtr)
	}
}
