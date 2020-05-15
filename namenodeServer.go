package main

import (
	"./namenode"
	"./utils"
	"net"
	"net/http"
	"net/rpc"
)

func main() {
	nameNodeInstance := new(namenode.Service)

	err := rpc.Register(nameNodeInstance)
	utils.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":1234")
	utils.Check(err)

	err = http.Serve(listener, nil)
	utils.Check(err)
}
