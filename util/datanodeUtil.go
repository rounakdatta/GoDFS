package util

import (
	"github.com/rounakdatta/GoDFS/datanode"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

func initializeDataNodeUtil(dataLocation string, serverPort string) {
	serverPortNum, _ := strconv.Atoi(serverPort)

	dataNodeInstance := new(datanode.Service)
	dataNodeInstance.DataDirectory = dataLocation
	dataNodeInstance.ServicePort = uint16(serverPortNum)

	err := rpc.Register(dataNodeInstance)
	Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":" + serverPort)
	Check(err)

	err = http.Serve(listener, nil)
	Check(err)
}
