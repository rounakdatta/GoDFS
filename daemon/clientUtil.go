package daemon

import (
	"github.com/rounakdatta/GoDFS/client"
	"github.com/rounakdatta/GoDFS/util"
	"log"
	"net/http"
	"net/rpc"
	"strconv"
)

var nameNodeInstance *rpc.Client

func putHandler(w http.ResponseWriter, req *http.Request) {
	sourcePath, ok := req.URL.Query()["sourcePath"]
	util.CheckStatus(ok)
	fileName, ok := req.URL.Query()["fileName"]
	util.CheckStatus(ok)

	client.Put(nameNodeInstance, sourcePath[0], fileName[0])
}

func getHandler(w http.ResponseWriter, req *http.Request) {
	fileName, ok := req.URL.Query()["fileName"]
	util.CheckStatus(ok)

	client.Get(nameNodeInstance, fileName[0])
}

func InitializeClientUtil(serverPort int, nameNodeAddress int) {
	var err error
	nameNodeInstance, err = rpc.Dial("tcp", strconv.Itoa(nameNodeAddress))
	util.Check(err)

	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/get", getHandler)

	http.ListenAndServe(":"+strconv.Itoa(serverPort), nil)

	log.Println("Client daemon HTTP started on port: " + strconv.Itoa(serverPort))
}
