package client

import (
	"net/rpc"
	"os"

	"UFS/datanode"
	"UFS/namenode"
	"UFS/util"
)

func Put(nameNodeInstance *rpc.Client, clientPath util.ClientPath) (putStatus bool) {
	// we deal with client files, so no data directory
	fullPath := clientPath.SourcePath + clientPath.FileName
	fileSizeHandler, err := os.Stat(fullPath)
	util.Check(err)

	fileSize := uint64(fileSizeHandler.Size())
	request := namenode.NameNodeWriteRequest{ClientPathVar: clientPath, FileSize: fileSize}
	var reply []namenode.NameNodeMetaData

	err = nameNodeInstance.Call("Service.WriteData", request, &reply)
	util.Check(err)

	var blockSize uint64
	err = nameNodeInstance.Call("Service.GetBlockSize", true, &blockSize)
	util.Check(err)

	fileHandler, err := os.Open(fullPath)
	util.Check(err)

	dataStagingBytes := make([]byte, blockSize)
	for _, metaData := range reply {
		n, err := fileHandler.Read(dataStagingBytes)
		util.Check(err)
		dataStagingBytes = dataStagingBytes[:n]

		blockPath := metaData.BlockPathVar
		blockAddresses := metaData.BlockAddresses

		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()

		request := datanode.DataNodePutRequest{
			BlockPathVar:     blockPath,
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

func Putd(nameNodeInstance *rpc.Client, clientPath util.ClientPath) (putStatus bool) {
	request := namenode.NameNodeWriteRequest{ClientPathVar: clientPath, FileSize: 0}
	var reply []namenode.NameNodeMetaData

	err := nameNodeInstance.Call("Service.WriteData", request, &reply)
	util.Check(err)

	for _, metaData := range reply {
		blockPath := metaData.BlockPathVar
		blockAddresses := metaData.BlockAddresses

		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()

		request := datanode.DataNodePutRequest{
			BlockPathVar:     blockPath,
			Data:             "",
			ReplicationNodes: remainingDataNodes,
		}
		var reply datanode.DataNodeWriteStatus

		rpcErr = dataNodeInstance.Call("Service.PutData", request, &reply)
		util.Check(rpcErr)
		putStatus = true
	}
	return
}

func Get(nameNodeInstance *rpc.Client, clientPath util.ClientPath) (fileContents string, getStatus bool) {
	request := namenode.NameNodeReadRequest{ClientPathVar: clientPath}
	var reply []namenode.NameNodeMetaData

	err := nameNodeInstance.Call("Service.ReadData", request, &reply)
	util.Check(err)

	fileContents = ""

	for _, metaData := range reply {
		blockPath := metaData.BlockPathVar
		blockAddresses := metaData.BlockAddresses
		blockFetchStatus := false

		for _, selectedDataNode := range blockAddresses {
			dataNodeInstance, rpcErr := rpc.Dial("tcp", selectedDataNode.Host+":"+selectedDataNode.ServicePort)
			if rpcErr != nil {
				continue
			}

			defer dataNodeInstance.Close()

			request := datanode.DataNodeGetRequest{
				BlockPathVar: blockPath,
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

func Ls(nameNodeInstance *rpc.Client, dirPath string) string {
	request := namenode.NameNodeLsRequest{DirPath: dirPath}
	var reply string
	err := nameNodeInstance.Call("Service.Ls", request, &reply)
	util.Check(err)
	return reply
}

func Search(nameNodeInstance *rpc.Client, clientPath util.ClientPath) string {
	request := namenode.NameNodeSearchRequest{ClientPathVar: clientPath}
	var reply string
	err := nameNodeInstance.Call("Service.Search", request, &reply)
	util.Check(err)
	return reply
}
