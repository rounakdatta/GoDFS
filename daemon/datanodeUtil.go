package daemon

import (
	"github.com/rounakdatta/GoDFS/datanode"
	"github.com/rounakdatta/GoDFS/util"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

func InitializeDataNodeUtil(serverPort int, dataLocation string) {
	dataNodeInstance := new(datanode.Service)
	dataNodeInstance.DataDirectory = dataLocation
	dataNodeInstance.ServicePort = uint16(serverPort)

	log.Printf("Data storage location is %s\n", dataLocation)
	log.Printf("DataNode port is %d\n", serverPort)

	err := rpc.Register(dataNodeInstance)
	util.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(serverPort))
	util.Check(err)

	err = http.Serve(listener, nil)
	util.Check(err)

	log.Println("DataNode daemon started on port: " + strconv.Itoa(serverPort))
}