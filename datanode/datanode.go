package datanode

import (
	"bufio"
	"errors"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"

	"UFS/util"
)

type Service struct {
	DataDirectory string
	ServicePort   uint16
	NameNodeHost  string
	NameNodePort  uint16
}

type DataNodePutRequest struct {
	BlockPathVar     util.BlockPath
	Data             string
	ReplicationNodes []util.DataNodeInstance
}

type DataNodeGetRequest struct {
	BlockPathVar util.BlockPath
}

type DataNodeWriteStatus struct {
	Status bool
}

type DataNodeData struct {
	Data string
}

type NameNodePingRequest struct {
	Host string
	Port uint16
}

type NameNodePingResponse struct {
	Ack bool
}

func (dataNode *Service) Ping(request *NameNodePingRequest, reply *NameNodePingResponse) error {
	dataNode.NameNodeHost = request.Host
	dataNode.NameNodePort = request.Port
	log.Printf("Received ping from NameNode, recorded as {NameNodeHost: %s, NameNodePort: %d}\n", dataNode.NameNodeHost, dataNode.NameNodePort)

	*reply = NameNodePingResponse{Ack: true}
	return nil
}

func (dataNode *Service) Heartbeat(request bool, response *bool) error {
	if request {
		log.Println("Received heartbeat from NameNode")
		*response = true
		return nil
	}
	return errors.New("HeartBeatError")
}

func (dataNode *Service) forwardForReplication(request *DataNodePutRequest, reply *DataNodeWriteStatus) error {
	blockAddresses := request.ReplicationNodes

	if len(blockAddresses) == 0 {
		return nil
	}

	startingDataNode := blockAddresses[0]
	remainingDataNodes := blockAddresses[1:]

	dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
	util.Check(rpcErr)
	defer dataNodeInstance.Close()

	payloadRequest := DataNodePutRequest{
		BlockPathVar:     request.BlockPathVar,
		Data:             request.Data,
		ReplicationNodes: remainingDataNodes,
	}

	rpcErr = dataNodeInstance.Call("Service.PutData", payloadRequest, &reply)
	util.Check(rpcErr)
	return nil
}

func (dataNode *Service) PutData(request *DataNodePutRequest, reply *DataNodeWriteStatus) error {
	// here we construct the data node path, including the data directory
	dirPath := dataNode.DataDirectory + "/" + request.BlockPathVar.MachineName + request.BlockPathVar.SourcePath

	err := os.MkdirAll(dirPath, 0777)
	util.Check(err)

	if request.BlockPathVar.BlockId == "" { // for directories, no data to write
		return dataNode.forwardForReplication(request, reply)
	}

	fullPath := dirPath + request.BlockPathVar.BlockId
	fileWriteHandler, err := os.Create(fullPath)

	util.Check(err)
	defer fileWriteHandler.Close()

	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(request.Data)
	util.Check(err)
	fileWriter.Flush()
	*reply = DataNodeWriteStatus{Status: true}

	return dataNode.forwardForReplication(request, reply)
}

func (dataNode *Service) GetData(request *DataNodeGetRequest, reply *DataNodeData) error {
	// here we construct the data node path, including the data directory
	fullPath := dataNode.DataDirectory + "/" + request.BlockPathVar.MachineName + request.BlockPathVar.SourcePath + request.BlockPathVar.BlockId

	dataBytes, err := ioutil.ReadFile(fullPath)
	util.Check(err)

	*reply = DataNodeData{Data: string(dataBytes)}
	return nil
}
