package client

import (
	"log"
	"net"
	"net/rpc"

	"UFS/client"
	"UFS/util"
)

func PutHandler(nameNodeAddress string, clientPath util.ClientPath) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Put(rpcClient, clientPath)
}

func PutdHandler(nameNodeAddress string, clientPath util.ClientPath) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Putd(rpcClient, clientPath)
}

func GetHandler(nameNodeAddress string, clientPath util.ClientPath) (string, bool) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Get(rpcClient, clientPath)
}

func LsHandler(nameNodeAddress string, dirPath string) string {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Ls(rpcClient, dirPath)
}

func SearchHandler(nameNodeAddress string, clientPath util.ClientPath) string {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Search(rpcClient, clientPath)
}

func initializeClientUtil(nameNodeAddress string) (*rpc.Client, error) {
	host, port, err := net.SplitHostPort(nameNodeAddress)
	util.Check(err)
	log.Printf("NameNode to connect to is %s\n", nameNodeAddress)
	return rpc.Dial("tcp", host+":"+port)
}
