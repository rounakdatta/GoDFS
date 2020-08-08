package client

import (
	"github.com/rounakdatta/GoDFS/datanode"
	"github.com/rounakdatta/GoDFS/namenode"
	"github.com/rounakdatta/GoDFS/util"
	"net/rpc"
	"os"
)

func Put(nameNodeInstance *rpc.Client, sourcePath string, fileName string) (putStatus bool) {
	fullFilePath := sourcePath + fileName
	fileSizeHandler, err := os.Stat(fullFilePath)
	util.Check(err)

	fileSize := uint64(fileSizeHandler.Size())
	request := namenode.NameNodeWriteRequest{FileName: fileName, FileSize: fileSize}
	var reply []namenode.NameNodeMetaData

	err = nameNodeInstance.Call("Service.WriteData", request, &reply)
	util.Check(err)

	var blockSize uint64
	err = nameNodeInstance.Call("Service.GetBlockSize", true, &blockSize)
	util.Check(err)

	fileHandler, err := os.Open(fullFilePath)
	util.Check(err)

	dataStagingBytes := make([]byte, blockSize)
	for _, metaData := range reply {
		n, err := fileHandler.Read(dataStagingBytes)
		util.Check(err)
		dataStagingBytes = dataStagingBytes[:n]

		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses

		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()

		request := datanode.DataNodePutRequest{
			BlockId:          blockId,
			Data:             string(dataStagingBytes),
			ReplicationNodes: remainingDataNodes,
		}
		var reply datanode.DataNodeWriteStatus

		rpcErr = dataNodeInstance.Call("Service.PutData", request, &reply)
		util.Check(rpcErr)
		putStatus = true
	}
	return
}

func Get(nameNodeInstance *rpc.Client, fileName string) (fileContents string, getStatus bool) {
	request := namenode.NameNodeReadRequest{FileName: fileName}
	var reply []namenode.NameNodeMetaData

	err := nameNodeInstance.Call("Service.ReadData", request, &reply)
	util.Check(err)

	fileContents = ""

	for _, metaData := range reply {
		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses
		blockFetchStatus := false

		for _, selectedDataNode := range blockAddresses {
			dataNodeInstance, rpcErr := rpc.Dial("tcp", selectedDataNode.Host+":"+selectedDataNode.ServicePort)
			if rpcErr != nil {
				continue
			}

			defer dataNodeInstance.Close()

			request := datanode.DataNodeGetRequest{
				BlockId: blockId,
			}
			var reply datanode.DataNodeData

			rpcErr = dataNodeInstance.Call("Service.GetData", request, &reply)
			util.Check(rpcErr)
			fileContents += reply.Data
			blockFetchStatus = true
			break
		}

		if !blockFetchStatus {
			getStatus = false
			return
		}
	}

	getStatus = true
	return
}
