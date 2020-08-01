package util

import (
	"github.com/rounakdatta/GoDFS/namenode"
	"net"
	"net/http"
	"net/rpc"
)

func discoverDataNodes(nameNodeInstance *namenode.Service, listOfDataNodes []string) error {
	nameNodeInstance.IdToDataNodes = make(map[uint64]DataNodeInstance)

	var i uint64
	availableNumberOfDataNodes := uint64(len(listOfDataNodes))
	for i = 0; i < availableNumberOfDataNodes; i++ {
		host, port, err := net.SplitHostPort(listOfDataNodes[i])
		Check(err)
		dataNodeInstance := DataNodeInstance{Host: host, ServicePort: port}
		nameNodeInstance.IdToDataNodes[i] = dataNodeInstance
	}

	return nil
}

func initializeNameNodeUtil(serverPort string, listOfDataNodes []string) {
	nameNodeInstance := new(namenode.Service)
	err := discoverDataNodes(nameNodeInstance, listOfDataNodes)
	Check(err)

	err = rpc.Register(nameNodeInstance)
	Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":" + serverPort)
	Check(err)

	err = http.Serve(listener, nil)
	Check(err)
}
