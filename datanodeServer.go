package main

import (
	"./datanode"
	"./utils"
	"net"
	"net/http"
	"net/rpc"
)

func main() {
	nameNodeInstance := new(datanode.Service)

	err := rpc.Register(nameNodeInstance)
	utils.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":4321")
	utils.Check(err)

	err = http.Serve(listener, nil)
	utils.Check(err)
}
