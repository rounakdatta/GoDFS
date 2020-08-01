package util

import (
	"../client"
	"net/http"
	"net/rpc"
)

var nameNodeInstance *rpc.Client

func putHandler(w http.ResponseWriter, req *http.Request) {
	sourcePath, ok := req.URL.Query()["sourcePath"]
	CheckStatus(ok)
	fileName, ok := req.URL.Query()["fileName"]
	CheckStatus(ok)

	client.Put(nameNodeInstance, sourcePath[0], fileName[0])
}

func getHandler(w http.ResponseWriter, req *http.Request) {
	fileName, ok := req.URL.Query()["fileName"]
	CheckStatus(ok)

	client.Get(nameNodeInstance, fileName[0])
}

func initializeClientUtil(nameNodeAddress string) {
	var err error
	nameNodeInstance, err = rpc.Dial("tcp", nameNodeAddress)
	Check(err)

	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/get", getHandler)

	http.ListenAndServe(":8000", nil)
}