package datanode

import (
	"bufio"
	"errors"
	"github.com/rounakdatta/GoDFS/util"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

type Service struct {
	DataDirectory string
	ServicePort   uint16
	NameNodeHost  string
	NameNodePort  uint16
}

type DataNodePutRequest struct {
	BlockId          string
	Data             string
	ReplicationNodes []util.DataNodeInstance
}

type DataNodeGetRequest struct {
	BlockId string
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
	blockId := request.BlockId
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
		BlockId:          blockId,
		Data:             request.Data,
		ReplicationNodes: remainingDataNodes,
	}

	rpcErr = dataNodeInstance.Call("Service.PutData", payloadRequest, &reply)
	util.Check(rpcErr)
	return nil
}

func (dataNode *Service) PutData(request *DataNodePutRequest, reply *DataNodeWriteStatus) error {
	fileWriteHandler, err := os.Create(dataNode.DataDirectory + request.BlockId)
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
	dataBytes, err := ioutil.ReadFile(dataNode.DataDirectory + request.BlockId)
	util.Check(err)

	*reply = DataNodeData{Data: string(dataBytes)}
	return nil
}
