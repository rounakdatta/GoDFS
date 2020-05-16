package client

import (
	"../datanode"
	"../namenode"
	"../utils"
	"net/rpc"
	"os"
)

func put(nameNodeInstance *rpc.Client, sourcePath string, fileName string) (putStatus bool) {
	fileSizeHandler, err := os.Stat(sourcePath)
	utils.Check(err)

	fileSize := uint64(fileSizeHandler.Size())
	request := namenode.NameNodeWriteRequest{FileName: fileName, FileSize: fileSize}
	var reply []namenode.NameNodeMetaData

	err = nameNodeInstance.Call("Service.WriteData", request, &reply)
	utils.Check(err)

	var blockSize uint64
	err = nameNodeInstance.Call("Service.GetBlockSize", true, blockSize)
	utils.Check(err)

	fileHandler, err := os.Open(sourcePath)
	utils.Check(err)

	dataStagingBytes := make([]byte, blockSize)
	for _, metaData := range reply {
		fileHandler.Read(dataStagingBytes)
		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses

		startingDataNode := blockAddresses[0]
		var remainingDataNodes []datanode.Service
		for _, instances := range blockAddresses[1:] {
			remainingDataNodes = append(remainingDataNodes, instances.DataNode)
		}

		dataNodeInstance, err := rpc.Dial("tcp", string(startingDataNode.DataNode.ServicePort))
		utils.Check(err)

		request := datanode.DataNodePutRequest{BlockId: blockId, Data: string(dataStagingBytes), ReplicationNodes: remainingDataNodes}
		var reply datanode.DataNodeWriteStatus
		err = dataNodeInstance.Call("Service.PutData", request, reply)
		utils.Check(err)
		putStatus = true
	}
	return
}

func get(nameNode *rpc.Client, fileName string) {

}

func main() {
	nameNodeInstance, err := rpc.Dial("tcp", "localhost:1234")
	utils.Check(err)

	if len(os.Args) < 2 {
		os.Exit(1)
	}
	request := os.Args[1]
	switch request {
	case "put":
		sourcePath := os.Args[2]
		fileName := os.Args[3]
		put(nameNodeInstance, sourcePath, fileName)
	case "get":
		fileName := os.Args[2]
		get(nameNodeInstance, fileName)
	default:
		os.Exit(1)
	}
}