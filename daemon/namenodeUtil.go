package daemon

import (
	"github.com/rounakdatta/GoDFS/namenode"
	"github.com/rounakdatta/GoDFS/util"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

func discoverDataNodes(nameNodeInstance *namenode.Service, listOfDataNodes []string) error {
	nameNodeInstance.IdToDataNodes = make(map[uint64]util.DataNodeInstance)

	var i uint64
	availableNumberOfDataNodes := uint64(len(listOfDataNodes))
	for i = 0; i < availableNumberOfDataNodes; i++ {
		host, port, err := net.SplitHostPort(listOfDataNodes[i])
		util.Check(err)
		dataNodeInstance := util.DataNodeInstance{Host: host, ServicePort: port}
		nameNodeInstance.IdToDataNodes[i] = dataNodeInstance
	}

	return nil
}

func InitializeNameNodeUtil(serverPort int, listOfDataNodes []string) {
	nameNodeInstance := new(namenode.Service)
	err := discoverDataNodes(nameNodeInstance, listOfDataNodes)
	util.Check(err)

	err = rpc.Register(nameNodeInstance)
	util.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(serverPort))
	util.Check(err)

	err = http.Serve(listener, nil)
	util.Check(err)

	log.Println("DataNode daemon started on port: " + strconv.Itoa(serverPort))
}
