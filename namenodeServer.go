package main

import (
	"./namenode"
	"./utils"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

func discoverDataNodes(nameNodeInstance *namenode.Service, listOfDataNodes []string) error {
	nameNodeInstance.IdToDataNodes = make(map[uint64]utils.DataNodeInstance)

	var i uint64
	availableNumberOfDataNodes := uint64(len(listOfDataNodes))
	for i = 0; i < availableNumberOfDataNodes; i++ {
		host, port, err := net.SplitHostPort(listOfDataNodes[i])
		utils.Check(err)
		dataNodeInstance := utils.DataNodeInstance{Host: host, ServicePort: port}
		nameNodeInstance.IdToDataNodes[i] = dataNodeInstance
	}

	return nil
}

func main() {
	if len(os.Args) < 3 {
		os.Exit(1)
	}
	serverPort := os.Args[1]
	listOfDataNodes := os.Args[2:]

	nameNodeInstance := new(namenode.Service)
	err := discoverDataNodes(nameNodeInstance, listOfDataNodes)
	utils.Check(err)

	err = rpc.Register(nameNodeInstance)
	utils.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":" + serverPort)
	utils.Check(err)

	err = http.Serve(listener, nil)
	utils.Check(err)
}
