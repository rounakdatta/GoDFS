package namenode

import (
	"github.com/google/uuid"
	"github.com/rounakdatta/GoDFS/util"
	"math"
	"math/rand"
)

type NameNodeMetaData struct {
	BlockId        string
	BlockAddresses []util.DataNodeInstance
}

type NameNodeReadRequest struct {
	FileName string
}

type NameNodeWriteRequest struct {
	FileName string
	FileSize uint64
}

type Service struct {
	Port uint16
	BlockSize          uint64
	ReplicationFactor  uint64
	IdToDataNodes      map[uint64]util.DataNodeInstance
	FileNameToBlocks   map[string][]string
	BlockToDataNodeIds map[string][]uint64
}

func NewService(blockSize uint64, replicationFactor uint64, serverPort uint16) *Service {
	return &Service{
		Port: serverPort,
		BlockSize:          blockSize,
		ReplicationFactor:  replicationFactor,
		FileNameToBlocks:   make(map[string][]string),
		IdToDataNodes:      make(map[uint64]util.DataNodeInstance),
		BlockToDataNodeIds: make(map[string][]uint64),
	}
}

func selectRandomNumbers(n uint64, count uint64) (randomNumberSet []uint64) {
	numberPresentMap := make(map[uint64]bool)
	for i := uint64(0); i < count; {
		generatedNumber := uint64(rand.Int63n(int64(n)))
		if _, ok := numberPresentMap[generatedNumber]; !ok {
			numberPresentMap[generatedNumber] = true
			randomNumberSet = append(randomNumberSet, generatedNumber)
			i++
		}
	}
	return
}

func (nameNode *Service) GetBlockSize(request bool, reply *uint64) error {
	if request {
		*reply = nameNode.BlockSize
	}
	return nil
}

func (nameNode *Service) HeartbeatToNameNode() error {
	return nil
}

func (nameNode *Service) ReadData(request *NameNodeReadRequest, reply *[]NameNodeMetaData) error {
	fileBlocks := nameNode.FileNameToBlocks[request.FileName]

	for _, block := range fileBlocks {
		var blockAddresses []util.DataNodeInstance

		targetDataNodeIds := nameNode.BlockToDataNodeIds[block]
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		*reply = append(*reply, NameNodeMetaData{BlockId: block, BlockAddresses: blockAddresses})
	}
	return nil
}

func (nameNode *Service) WriteData(request *NameNodeWriteRequest, reply *[]NameNodeMetaData) error {
	nameNode.FileNameToBlocks[request.FileName] = []string{}

	numberOfBlocksToAllocate := uint64(math.Ceil(float64(request.FileSize) / float64(nameNode.BlockSize)))
	*reply = nameNode.allocateBlocks(request.FileName, numberOfBlocksToAllocate)
	return nil
}

func (nameNode *Service) allocateBlocks(fileName string, numberOfBlocks uint64) (metadata []NameNodeMetaData) {
	nameNode.FileNameToBlocks[fileName] = []string{}
	dataNodesAvailable := uint64(len(nameNode.IdToDataNodes))

	for i := uint64(0); i < numberOfBlocks; i++ {
		blockId := uuid.New().String()
		nameNode.FileNameToBlocks[fileName] = append(nameNode.FileNameToBlocks[fileName], blockId)

		var blockAddresses []util.DataNodeInstance
		var replicationFactor uint64
		if nameNode.ReplicationFactor > dataNodesAvailable {
			replicationFactor = dataNodesAvailable
		} else {
			replicationFactor = nameNode.ReplicationFactor
		}

		targetDataNodeIds := selectRandomNumbers(dataNodesAvailable, replicationFactor)
		nameNode.BlockToDataNodeIds[blockId] = targetDataNodeIds
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		metadata = append(metadata, NameNodeMetaData{BlockId: blockId, BlockAddresses: blockAddresses})
	}
	return
}
