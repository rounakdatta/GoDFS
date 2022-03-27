package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"

	"UFS/daemon/client"
	"UFS/daemon/datanode"
	"UFS/daemon/namenode"
	"UFS/util"
)

func main() {
	dataNodeCommand := flag.NewFlagSet("datanode", flag.ExitOnError)
	nameNodeCommand := flag.NewFlagSet("namenode", flag.ExitOnError)
	clientCommand := flag.NewFlagSet("client", flag.ExitOnError)

	dataNodePortPtr := dataNodeCommand.Int("port", 7000, "DataNode communication port")
	dataNodeDataLocationPtr := dataNodeCommand.String("data-location", ".dndata", "DataNode data storage location")

	nameNodePortPtr := nameNodeCommand.Int("port", 9000, "NameNode communication port")
	nameNodeListPtr := nameNodeCommand.String("datanodes", "", "Comma-separated list of DataNodes to connect to")
	nameNodeBlockSizePtr := nameNodeCommand.Int("block-size", 32, "Block size to store")
	nameNodeReplicationFactorPtr := nameNodeCommand.Int("replication-factor", 1, "Replication factor of the system")

	clientNameNodePortPtr := clientCommand.String("namenode", "localhost:9000", "NameNode communication port")
	clientOperationPtr := clientCommand.String("operation", "", "Operation to perform")
	clientSourcePathPtr := clientCommand.String("source-path", "", "Source path of the file")
	clientFilenamePtr := clientCommand.String("filename", "", "File name")

	if len(os.Args) < 4 {
		log.Println("sub-command is required")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "datanode":
		_ = dataNodeCommand.Parse(os.Args[2:])
		datanode.InitializeDataNodeUtil(*dataNodePortPtr, *dataNodeDataLocationPtr)

	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		var listOfDataNodes []string
		if len(*nameNodeListPtr) > 1 {
			listOfDataNodes = strings.Split(*nameNodeListPtr, ",")
		} else {
			listOfDataNodes = []string{}
		}
		namenode.InitializeNameNodeUtil(*nameNodePortPtr, *nameNodeBlockSizePtr, *nameNodeReplicationFactorPtr, listOfDataNodes)

	case "client":
		_ = clientCommand.Parse(os.Args[2:])

		if *clientOperationPtr == "put" {
			sourcePath, err := filepath.Abs(*clientSourcePathPtr)
			util.Check(err)
			if sourcePath[len(sourcePath)-1:] != "/" {
				sourcePath += "/"
			}
			// we will fetch the data directory later
			// now we take only the client path
			clientPath := util.ClientPath{MachineName: "Cox", SourcePath: sourcePath, FileName: *clientFilenamePtr}

			status := client.PutHandler(*clientNameNodePortPtr, clientPath)
			log.Printf("Put status: %t\n", status)

		} else if *clientOperationPtr == "putd" {
			sourcePath, err := filepath.Abs(*clientSourcePathPtr)
			util.Check(err)
			if sourcePath[len(sourcePath)-1:] != "/" {
				sourcePath += "/"
			}
			// we will fetch the data directory later
			// now we take only the client path
			clientPath := util.ClientPath{MachineName: "Cox", SourcePath: sourcePath, FileName: ""}

			status := client.PutdHandler(*clientNameNodePortPtr, clientPath)
			log.Printf("Putd status: %t\n", status)
		} else if *clientOperationPtr == "get" {
			sourcePath, err := filepath.Abs(*clientSourcePathPtr)
			util.Check(err)
			if sourcePath[len(sourcePath)-1:] != "/" {
				sourcePath += "/"
			}
			// we will fetch the data directory later
			// now we take only the client path
			clientPath := util.ClientPath{MachineName: "Cox", SourcePath: sourcePath, FileName: *clientFilenamePtr}

			contents, status := client.GetHandler(*clientNameNodePortPtr, clientPath)
			log.Printf("Get status: %t\n", status)
			if status {
				log.Println(contents)
			}
		} else if *clientOperationPtr == "ls" {
			// ls receives the absolute directory path as an argument
			if len(os.Args) > 7 {
				log.Println("Syntax is: ls <absolute_path>")
				os.Exit(1)
			}

			if len(os.Args) <= 6 {
				log.Println("Syntax is: ls <absolute_path>")
				os.Exit(1)
			}

			content := client.LsHandler(*clientNameNodePortPtr, os.Args[6])
			log.Println(content)
		} else if *clientOperationPtr == "search" {
			sourcePath, err := filepath.Abs(*clientSourcePathPtr)
			util.Check(err)
			if sourcePath[len(sourcePath)-1:] != "/" {
				sourcePath += "/"
			}
			// we will fetch the data directory later
			// now we take only the client path
			clientPath := util.ClientPath{MachineName: "Cox", SourcePath: sourcePath, FileName: *clientFilenamePtr}

			contents := client.SearchHandler(*clientNameNodePortPtr, clientPath)
			log.Println(contents)
		}
	}
}
