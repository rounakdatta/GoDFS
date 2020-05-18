package namenode

import (
	"../utils"
	"github.com/google/uuid"
	"math"
	"math/rand"
)

type NameNodeMetaData struct {
	BlockId string
	BlockAddresses []utils.DataNodeInstance
}

type NameNodeReadRequest struct {
	FileName string
}

type NameNodeWriteRequest struct {
	FileName string
	FileSize uint64
}

type Service struct {
	BlockSize          uint64
	ReplicationFactor  uint8
	IdToDataNodes      map[uint64]utils.DataNodeInstance
	FileNameToBlocks   map[string][]string
	BlockToDataNodeIds map[string][]uint64
}

func selectRandomNumbers(n uint64, count uint8)(randomNumberSet []uint64) {
	numberPresentMap := make(map[uint64]bool)
	for i := uint8(0); i < count ; {
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

func (nameNode *Service) ReadData(request *NameNodeReadRequest, reply *[]NameNodeMetaData) error {
	fileBlocks := nameNode.FileNameToBlocks[request.FileName]

	for _, block := range fileBlocks {
		var blockAddresses []utils.DataNodeInstance

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

	numberOfBlocksToAllocate := uint64(math.Ceil(float64(request.FileSize / nameNode.BlockSize)))
	*reply = nameNode.allocateBlocks(request.FileName, numberOfBlocksToAllocate)
	return nil
}

func (nameNode *Service) allocateBlocks(fileName string, numberOfBlocks uint64)(metadata []NameNodeMetaData) {
	nameNode.FileNameToBlocks[fileName] = []string{}
	dataNodesAvailable := uint64(len(nameNode.IdToDataNodes))

	for i := uint64(0); i < numberOfBlocks; i++ {
		blockId := uuid.New().String()
		nameNode.FileNameToBlocks[fileName] = append(nameNode.FileNameToBlocks[fileName], blockId)

		var blockAddresses []utils.DataNodeInstance

		targetDataNodeIds := selectRandomNumbers(dataNodesAvailable, nameNode.ReplicationFactor)
		nameNode.BlockToDataNodeIds[blockId] = targetDataNodeIds
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		metadata = append(metadata, NameNodeMetaData{BlockId: blockId, BlockAddresses: blockAddresses})
	}
	return
}