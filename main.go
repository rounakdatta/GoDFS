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
	clientOperationPtr := clientCommand.String("operation", "", "Operation to perform")

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
		configurations := nameNodeCommand.Args()

		if len(configurations) != 2 {
			log.Println("incorrect number of configurations, required: blockSize, replicationFactor")
			os.Exit(1)
		}

		listOfDataNodes := strings.Split(*nameNodeListPtr, ",")
		daemon.InitializeNameNodeUtil(*nameNodePortPtr, configurations[0], configurations[1], listOfDataNodes)

	case "client":
		_ = clientCommand.Parse(os.Args[2:])
		configurations := clientCommand.Args()

		if *clientOperationPtr == "put" {
			if len(configurations) != 2 {
				log.Println("incorrect number of configurations, required: sourcePath, fileName")
				os.Exit(1)
			}

			daemon.PutHandler(*clientNameNodePortPtr, configurations[0], configurations[1])
		} else if *clientOperationPtr == "get" {
			if len(configurations) != 1 {
				log.Println("incorrect number of configurations, required: fileName")
				os.Exit(1)
			}

			daemon.GetHandler(*clientNameNodePortPtr, configurations[0])
		}
	}
}
