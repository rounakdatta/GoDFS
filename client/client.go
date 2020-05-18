package client

import (
	"../datanode"
	"../namenode"
	"../utils"
	"net/rpc"
	"os"
)

func Put(nameNodeInstance *rpc.Client, sourcePath string, fileName string) (putStatus bool) {
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
		remainingDataNodes := blockAddresses[1:]

		dataNodeInstance, err := rpc.Dial("tcp", startingDataNode.ServicePort)
		utils.Check(err)

		request := datanode.DataNodePutRequest{BlockId: blockId, Data: string(dataStagingBytes), ReplicationNodes: remainingDataNodes}
		var reply datanode.DataNodeWriteStatus
		err = dataNodeInstance.Call("Service.PutData", request, reply)
		utils.Check(err)
		putStatus = true
	}
	return
}

func Get(nameNode *rpc.Client, fileName string) {

}