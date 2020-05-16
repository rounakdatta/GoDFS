package main

import (
	"./datanode"
	"./utils"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) != 3 {
		os.Exit(1)
	}
	serverHost := os.Args[1]
	serverPort, _ := strconv.Atoi(os.Args[2])

	nameNodeInstance := datanode.Service{DataDirectory: serverHost, ServicePort: uint16(serverPort)}

	err := rpc.Register(nameNodeInstance)
	utils.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":" + string(nameNodeInstance.ServicePort))
	utils.Check(err)

	err = http.Serve(listener, nil)
	utils.Check(err)
}
