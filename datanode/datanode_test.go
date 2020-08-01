package datanode

import (
	"strings"
	"testing"
)

// Test creating a DataNode Service
func TestDataNodeServiceCreation(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000

	if testDataNodeService.DataDirectory != "./" {
		t.Errorf("Unable to set DataDirectory correctly; Expected: %s, found: %s", "./", testDataNodeService.DataDirectory)
	}
	if testDataNodeService.ServicePort != 8000 {
		t.Errorf("Unable to set ServicePort correctly; Expected: %d, found: %d", 8000, testDataNodeService.ServicePort)
	}
}

// Test writing data within DataNode
func TestDataNodeServiceWrite(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000

	putRequestPayload := DataNodePutRequest{BlockId: "1", Data: "Hello world", ReplicationNodes: nil}
	var replyPayload DataNodeWriteStatus
	testDataNodeService.PutData(&putRequestPayload, &replyPayload)

	if !replyPayload.Status {
		t.Errorf("Unable to write data correctly; Expected: %t, found: %t", true, replyPayload.Status)
	}
}

// Test reading data within DataNode
func TestDataNodeServiceRead(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000

	getRequestPayload := DataNodeGetRequest{BlockId: "1"}
	var replyPayload DataNodeData
	testDataNodeService.GetData(&getRequestPayload, &replyPayload)

	if strings.Compare(replyPayload.Data, "Hello world") != 0 {
		t.Errorf("Unable to read data correctly; Expected: %s, found: %s.", "Hello world", replyPayload.Data)
	}
}
