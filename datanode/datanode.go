package datanode

import (
	"bufio"
	"io/ioutil"
	"os"
)

type Service struct {
	dataDirectory string
	ServicePort   uint16
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func (dataNode *Service) forwardForReplication(blockId string, data string, replicationNodes []Service) {
}

func (dataNode *Service) putData(blockId string, data string, replicationNodes []Service) {
	fileWriteHandler, err := os.Create(dataNode.dataDirectory + blockId)
	check(err)
	defer fileWriteHandler.Close()

	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(data)
	check(err)
	fileWriter.Flush()

	dataNode.forwardForReplication(blockId, data, replicationNodes)
}

func (dataNode *Service) getData(blockId string) (data string) {
	dataBytes, err := ioutil.ReadFile(dataNode.dataDirectory + blockId)
	check(err)

	data = string(dataBytes)
	return data
}