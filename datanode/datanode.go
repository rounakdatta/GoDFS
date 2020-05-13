package main

import (
	"bufio"
	"io/ioutil"
	"os"
)
type DataNodeService struct {
	DataDirectory string
	ServicePort uint16
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func (dataNode *DataNodeService) forwardForReplication(blockId string, data string, replicationNodes []string) {
}

func (dataNode *DataNodeService) putData(blockId string, data string, replicationNodes []string) {
	fileWriteHandler, err := os.Create(dataNode.DataDirectory + blockId)
	check(err)
	defer fileWriteHandler.Close()

	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(data)
	check(err)
	fileWriter.Flush()

	dataNode.forwardForReplication(blockId, data, replicationNodes)
}

func (dataNode *DataNodeService) getData(blockId string) (data string) {
	dataBytes, err := ioutil.ReadFile(dataNode.DataDirectory + blockId)
	check(err)

	data = string(dataBytes)
	return data
}

func main() {
}