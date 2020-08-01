package datanode

import (
	"../util"
	"bufio"
	"io/ioutil"
	"os"
)

type Service struct {
	DataDirectory string
	ServicePort   uint16
}

type DataNodePutRequest struct {
	BlockId string
	Data string
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

func (dataNode *Service) forwardForReplication(request *DataNodePutRequest, reply *DataNodeWriteStatus) error {
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