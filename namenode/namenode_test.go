package namenode

import (
	"github.com/rounakdatta/GoDFS/util"
	"log"
	"testing"
)

// Test creating a NameNode Service
func TestNameNodeCreation(t *testing.T) {
	testNameNodeService := Service{
		BlockSize:          4,
		ReplicationFactor:  2,
		FileNameToBlocks:   make(map[string][]string),
		IdToDataNodes:      make(map[uint64]util.DataNodeInstance),
		BlockToDataNodeIds: make(map[string][]uint64),
	}

	testDataNodeInstance1 := util.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := util.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2

	if len(testNameNodeService.IdToDataNodes) != 2 || testNameNodeService.BlockSize != 4 || testNameNodeService.ReplicationFactor != 2 {
		t.Errorf("Unable to initialize NameNode correctly; Expected: %d, %d, %d, found: %v, %d %d.", 2, 4, 2, testNameNodeService.IdToDataNodes, testNameNodeService.BlockSize, testNameNodeService.ReplicationFactor)
	}
}

// Test write process
func TestNameNodeServiceWrite(t *testing.T) {
	testNameNodeService := Service{
		BlockSize:          4,
		ReplicationFactor:  2,
		FileNameToBlocks:   make(map[string][]string),
		IdToDataNodes:      make(map[uint64]util.DataNodeInstance),
		BlockToDataNodeIds: make(map[string][]uint64),
	}

	testDataNodeInstance1 := util.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := util.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2

	writeDataPayload := NameNodeWriteRequest{
		FileName: "foo",
		FileSize: 12,
	}

	var replyPayload []NameNodeMetaData
	err := testNameNodeService.WriteData(&writeDataPayload, &replyPayload)
	log.Println(replyPayload)
	util.Check(err)
	if len(replyPayload) != 3 {
		t.Errorf("Unable to set metadata correctly; Expected: %d, found: %d.", 3, len(replyPayload))
	}
}
