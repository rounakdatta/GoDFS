package daemon

import (
	"github.com/rounakdatta/GoDFS/client"
	"github.com/rounakdatta/GoDFS/util"
	"log"
	"net/rpc"
	"strconv"
)

func PutHandler(nameNodeAddress int, sourcePath string, fileName string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Put(rpcClient, sourcePath, fileName)
}

func GetHandler(nameNodeAddress int, fileName string) (string, bool) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Get(rpcClient, fileName)
}

func initializeClientUtil(nameNodeAddress int) (*rpc.Client, error) {
	log.Printf("NameNode to connect to is %d\n", nameNodeAddress)
	return rpc.Dial("tcp", "127.0.0.1:" + strconv.Itoa(nameNodeAddress))
}
