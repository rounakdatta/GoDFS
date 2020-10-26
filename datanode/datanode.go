package datanode

import (
	"bufio"
	"github.com/rounakdatta/GoDFS/util"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Service struct {
	DataDirectory string
	ServicePort   uint16
	NameNodeHost string
	NameNodePort uint16
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

func (dataNode *Service) PingToDataNode(request *NameNodePingRequest, reply *NameNodePingResponse) error {
	dataNode.NameNodeHost = request.Host
	dataNode.NameNodePort = request.Port
	log.Printf("Received ping from NameNode, recorded as {NameNodeHost: %s, NameNodePort: %d}\n", dataNode.NameNodeHost, dataNode.NameNodePort)

	// go dataNode.initiateHeartbeat()

	*reply = NameNodePingResponse{Ack: true}
	return nil
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

func (dataNode *Service) initiateHeartbeat() {
	go dataNode.heartbeat()
	time.Sleep(time.Minute * 10)
}

func (dataNode *Service) heartbeat() {
	var nameNodeClient *rpc.Client
	var err error
	for {
		nameNodeClient, err = rpc.Dial("tcp", dataNode.NameNodeHost+":"+strconv.Itoa(int(dataNode.NameNodePort)))
		if err == nil {
			break
		}
	}

	for range time.Tick(time.Second * 5) {
		var response bool
		_ = nameNodeClient.Call("Service.HeartbeatToNameNode", true, &response)
	}
}