package main

import (
	"./datanode"
	"./utils"
	"fmt"
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
	dataLocation := os.Args[1]
	serverPort, _ := strconv.Atoi(os.Args[2])
	fmt.Println(dataLocation)
	fmt.Println(serverPort)

	// dataNodeInstance := datanode.Service{DataDirectory: dataLocation, ServicePort: uint16(serverPort)}
	dataNodeInstance := new(datanode.Service)
	dataNodeInstance.DataDirectory = dataLocation
	dataNodeInstance.ServicePort = uint16(serverPort)

	err := rpc.Register(dataNodeInstance)
	utils.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":" + strconv.Itoa(serverPort))
	utils.Check(err)

	err = http.Serve(listener, nil)
	utils.Check(err)
}
