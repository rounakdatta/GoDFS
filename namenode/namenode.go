package namenode

import (
	"errors"
	"github.com/google/uuid"
	"github.com/rounakdatta/GoDFS/datanode"
	"github.com/rounakdatta/GoDFS/util"
	"log"
	"math"
	"math/rand"
	"net/rpc"
	"strings"
	"time"
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

type ReDistributeDataRequest struct {
	DataNodeUri string
}

type UnderReplicatedBlocks struct {
	BlockId           string
	HealthyDataNodeId uint64
}

type Service struct {
	Port               uint16
	BlockSize          uint64
	ReplicationFactor  uint64
	IdToDataNodes      map[uint64]util.DataNodeInstance
	FileNameToBlocks   map[string][]string
	BlockToDataNodeIds map[string][]uint64
}

func NewService(blockSize uint64, replicationFactor uint64, serverPort uint16) *Service {
	return &Service{
		Port:               serverPort,
		BlockSize:          blockSize,
		ReplicationFactor:  replicationFactor,
		FileNameToBlocks:   make(map[string][]string),
		IdToDataNodes:      make(map[uint64]util.DataNodeInstance),
		BlockToDataNodeIds: make(map[string][]uint64),
	}
}

func selectRandomNumbers(availableItems []uint64, count uint64) (randomNumberSet []uint64) {
	numberPresentMap := make(map[uint64]bool)
	for i := uint64(0); i < count; {
		rand.Seed(time.Now().Unix())
		chosenItem := availableItems[rand.Intn(len(availableItems))]
		if _, ok := numberPresentMap[chosenItem]; !ok {
			numberPresentMap[chosenItem] = true
			randomNumberSet = append(randomNumberSet, chosenItem)
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
	var dataNodesAvailable []uint64
	for k, _ := range nameNode.IdToDataNodes {
		dataNodesAvailable = append(dataNodesAvailable, k)
	}
	dataNodesAvailableCount := uint64(len(dataNodesAvailable))

	for i := uint64(0); i < numberOfBlocks; i++ {
		blockId := uuid.New().String()
		nameNode.FileNameToBlocks[fileName] = append(nameNode.FileNameToBlocks[fileName], blockId)

		var blockAddresses []util.DataNodeInstance
		var replicationFactor uint64
		if nameNode.ReplicationFactor > dataNodesAvailableCount {
			replicationFactor = dataNodesAvailableCount
		} else {
			replicationFactor = nameNode.ReplicationFactor
		}

		targetDataNodeIds := nameNode.assignDataNodes(blockId, dataNodesAvailable, replicationFactor)
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		metadata = append(metadata, NameNodeMetaData{BlockId: blockId, BlockAddresses: blockAddresses})
	}
	return
}

func (nameNode *Service) assignDataNodes(blockId string, dataNodesAvailable []uint64, replicationFactor uint64) []uint64 {
	targetDataNodeIds := selectRandomNumbers(dataNodesAvailable, replicationFactor)
	nameNode.BlockToDataNodeIds[blockId] = targetDataNodeIds
	return targetDataNodeIds
}

func (nameNode *Service) ReDistributeData(request *ReDistributeDataRequest, reply *bool) error {
	log.Printf("DataNode %s is dead, trying to redistribute data\n", request.DataNodeUri)
	deadDataNodeSlice := strings.Split(request.DataNodeUri, ":")
	var deadDataNodeId uint64

	// de-register the dead DataNode from IdToDataNodes meta
	for id, dn := range nameNode.IdToDataNodes {
		if dn.Host == deadDataNodeSlice[0] && dn.ServicePort == deadDataNodeSlice[1] {
			deadDataNodeId = id
			break
		}
	}
	delete(nameNode.IdToDataNodes, deadDataNodeId)

	// construct under-replicated blocks list and
	// de-register the block entirely in favour of re-creation
	var underReplicatedBlocksList []UnderReplicatedBlocks
	for blockId, dnIds := range nameNode.BlockToDataNodeIds {
		for i, dnId := range dnIds {
			if dnId == deadDataNodeId {
				healthyDataNodeId := nameNode.BlockToDataNodeIds[blockId][(i+1)%len(dnIds)]
				underReplicatedBlocksList = append(
					underReplicatedBlocksList,
					UnderReplicatedBlocks{blockId, healthyDataNodeId},
				)
				// TODO: trigger data deletion on the existing data nodes
				break
			}
			delete(nameNode.BlockToDataNodeIds, blockId)
		}
	}

	// verify if re-replication would be possible
	if len(nameNode.IdToDataNodes) < int(nameNode.ReplicationFactor) {
		log.Println("Replication not possible due to unavailability of sufficient DataNode(s)")
		return errors.New("ReplicationNotPossible")
	}

	var availableNodes []uint64
	for k, _ := range nameNode.IdToDataNodes {
		availableNodes = append(availableNodes, k)
	}

	// attempt re-replication of under-replicated blocks
	for _, blockToReplicate := range underReplicatedBlocksList {

		// fetch the data from the healthy DataNode
		healthyDataNode := nameNode.IdToDataNodes[blockToReplicate.HealthyDataNodeId]
		dataNodeInstance, rpcErr := rpc.Dial("tcp", healthyDataNode.Host+":"+healthyDataNode.ServicePort)
		if rpcErr != nil {
			continue
		}

		defer dataNodeInstance.Close()

		getRequest := datanode.DataNodeGetRequest{
			BlockId: blockToReplicate.BlockId,
		}
		var getReply datanode.DataNodeData

		rpcErr = dataNodeInstance.Call("Service.GetData", getRequest, &getReply)
		util.Check(rpcErr)
		blockContents := getReply.Data

		// initiate the replication of the block contents
		targetDataNodeIds := nameNode.assignDataNodes(blockToReplicate.BlockId, availableNodes, nameNode.ReplicationFactor)
		var blockAddresses []util.DataNodeInstance
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}
		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		targetDataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		util.Check(rpcErr)
		defer targetDataNodeInstance.Close()

		putRequest := datanode.DataNodePutRequest{
			BlockId:          blockToReplicate.BlockId,
			Data:             blockContents,
			ReplicationNodes: remainingDataNodes,
		}
		var putReply datanode.DataNodeWriteStatus

		rpcErr = dataNodeInstance.Call("Service.PutData", putRequest, &putReply)
		util.Check(rpcErr)

		log.Printf("Block %s replication completed for %v\n", blockToReplicate.BlockId, targetDataNodeIds)
	}

	return nil
}
