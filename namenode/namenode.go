package namenode

import (
	"../datanode"
	"math"
	"github.com/google/uuid"
	"math/rand"
)

type DataNodeInstance struct {
	Host string
	DataNode datanode.Service
}

type Service struct {
	BlockSize          uint64
	ReplicationFactor  uint8
	IdToDataNodes      map[uint64]DataNodeInstance
	FileNameToBlocks   map[string][]string
	BlockToDataNodeIds map[string][]uint64
}

type MetaData struct {
	BlockId string
	BlockAddresses []DataNodeInstance
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

func (nameNode *Service) readData(fileName string)(metadata []MetaData) {
	fileBlocks := nameNode.FileNameToBlocks[fileName]

	for _, block := range fileBlocks {
		var blockAddresses []DataNodeInstance

		targetDataNodeIds := nameNode.BlockToDataNodeIds[block]
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		metadata = append(metadata, MetaData{block, blockAddresses})
	}
	return
}

func (nameNode *Service) writeData(fileName string, fileSize uint64) []MetaData {
	nameNode.FileNameToBlocks[fileName] = []string{}

	numberOfBlocksToAllocate := uint64(math.Ceil(float64(fileSize / nameNode.BlockSize)))
	return nameNode.allocateBlocks(fileName, numberOfBlocksToAllocate)
}

func (nameNode *Service) allocateBlocks(fileName string, numberOfBlocks uint64)(metadata []MetaData) {
	nameNode.FileNameToBlocks[fileName] = []string{}
	dataNodesAvailable := uint64(len(nameNode.IdToDataNodes))

	for i := uint64(0); i < numberOfBlocks; i++ {
		blockId := uuid.New().String()
		nameNode.FileNameToBlocks[fileName] = append(nameNode.FileNameToBlocks[fileName], blockId)

		var blockAddresses []DataNodeInstance

		targetDataNodeIds := selectRandomNumbers(dataNodesAvailable, nameNode.ReplicationFactor)
		nameNode.BlockToDataNodeIds[blockId] = targetDataNodeIds
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		metadata = append(metadata, MetaData{blockId, blockAddresses})
	}
	return
}