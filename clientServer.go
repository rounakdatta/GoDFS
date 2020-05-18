package main

import (
	"./client"
	"./utils"
	"net/http"
	"net/rpc"
	"os"
)

var nameNodeInstance *rpc.Client

func putHandler(w http.ResponseWriter, req *http.Request) {
	sourcePath, ok := req.URL.Query()["sourcePath"]
	utils.CheckStatus(ok)
	fileName, ok := req.URL.Query()["fileName"]
	utils.CheckStatus(ok)

	client.Put(nameNodeInstance, sourcePath[0], fileName[0])
}

func getHandler(w http.ResponseWriter, req *http.Request) {
	fileName, ok := req.URL.Query()["fileName"]
	utils.CheckStatus(ok)

	client.Get(nameNodeInstance, fileName[0])
}

func main() {
	if len(os.Args) < 2 {
		os.Exit(1)
	}
	nameNodeAddress := os.Args[1]

	var err error
	nameNodeInstance, err = rpc.Dial("tcp", nameNodeAddress)
	utils.Check(err)

	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/get", getHandler)

	http.ListenAndServe(":8000", nil)
}