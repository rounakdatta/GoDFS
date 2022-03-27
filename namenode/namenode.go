package namenode

import (
	"log"
	"math"
	"math/rand"
	"net/rpc"
	"strings"

	"UFS/datanode"
	"UFS/util"

	"github.com/google/uuid"
)

type NameNodeMetaData struct {
	BlockPathVar   util.BlockPath
	BlockAddresses []util.DataNodeInstance
}

type NameNodeReadRequest struct {
	ClientPathVar util.ClientPath
}

type NameNodeLsRequest struct {
	DirPath string
}

type NameNodeSearchRequest struct {
	ClientPathVar util.ClientPath
}

type NameNodeWriteRequest struct {
	ClientPathVar util.ClientPath
	FileSize      uint64
}

type ReDistributeDataRequest struct {
	DataNodeUri string
}

type UnderReplicatedBlocks struct {
	BlockPathVar      util.BlockPath
	HealthyDataNodeId uint64
}

type Service struct {
	Port                   uint16
	BlockSize              uint64
	ReplicationFactor      uint64
	IdToDataNodes          map[uint64]util.DataNodeInstance
	ClientPathToBlockPath  map[util.ClientPath][]util.BlockPath
	BlockPathToDataNodeIds map[util.BlockPath][]uint64
}

func NewService(blockSize uint64, replicationFactor uint64, serverPort uint16) *Service {
	return &Service{
		Port:                   serverPort,
		BlockSize:              blockSize,
		ReplicationFactor:      replicationFactor,
		ClientPathToBlockPath:  make(map[util.ClientPath][]util.BlockPath),
		IdToDataNodes:          make(map[uint64]util.DataNodeInstance),
		BlockPathToDataNodeIds: make(map[util.BlockPath][]uint64),
	}
}

func selectRandomNumbers(availableItems []uint64, count uint64) (randomNumberSet []uint64) {
	numberPresentMap := make(map[uint64]bool)
	for i := uint64(0); i < count; {
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
	fileBlocks := nameNode.ClientPathToBlockPath[request.ClientPathVar]

	for _, blockPath := range fileBlocks {
		var blockAddresses []util.DataNodeInstance

		targetDataNodeIds := nameNode.BlockPathToDataNodeIds[blockPath]
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		*reply = append(*reply, NameNodeMetaData{BlockPathVar: blockPath, BlockAddresses: blockAddresses})
	}
	return nil
}

func (nameNode *Service) WriteData(request *NameNodeWriteRequest, reply *[]NameNodeMetaData) error {
	nameNode.ClientPathToBlockPath[request.ClientPathVar] = []util.BlockPath{}

	var numberOfBlocksToAllocate uint64
	if request.FileSize != 0 { // we write a file
		numberOfBlocksToAllocate = uint64(math.Ceil(float64(request.FileSize) / float64(nameNode.BlockSize)))
	} else { // we create a directory
		numberOfBlocksToAllocate = uint64(1) // we want only one block, in particular, only the directory
	}

	*reply = nameNode.allocateBlocks(request.ClientPathVar, numberOfBlocksToAllocate)
	return nil
}

func (nameNode *Service) allocateBlocks(clientPath util.ClientPath, numberOfBlocks uint64) (metadata []NameNodeMetaData) {
	nameNode.ClientPathToBlockPath[clientPath] = []util.BlockPath{}

	var dataNodesAvailable []uint64
	for k, _ := range nameNode.IdToDataNodes {
		dataNodesAvailable = append(dataNodesAvailable, k)
	}
	dataNodesAvailableCount := uint64(len(dataNodesAvailable))

	for i := uint64(0); i < numberOfBlocks; i++ {
		var blockId string
		if clientPath.FileName != "" { // create Id only for files
			blockId = uuid.New().String()
		}

		// here we add the machine name to the block path
		blockPath := util.BlockPath{MachineName: clientPath.MachineName, SourcePath: clientPath.SourcePath, BlockId: blockId}
		nameNode.ClientPathToBlockPath[clientPath] = append(nameNode.ClientPathToBlockPath[clientPath], blockPath)

		var blockAddresses []util.DataNodeInstance
		var replicationFactor uint64
		if nameNode.ReplicationFactor > dataNodesAvailableCount {
			replicationFactor = dataNodesAvailableCount
		} else {
			replicationFactor = nameNode.ReplicationFactor
		}

		targetDataNodeIds := nameNode.assignDataNodes(blockPath, dataNodesAvailable, replicationFactor)
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		metadata = append(metadata, NameNodeMetaData{BlockPathVar: blockPath, BlockAddresses: blockAddresses})
	}
	return
}

func (nameNode *Service) assignDataNodes(blockPath util.BlockPath, dataNodesAvailable []uint64, replicationFactor uint64) []uint64 {
	targetDataNodeIds := selectRandomNumbers(dataNodesAvailable, replicationFactor)
	nameNode.BlockPathToDataNodeIds[blockPath] = targetDataNodeIds
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
	for blockPath, dnIds := range nameNode.BlockPathToDataNodeIds {
		for i, dnId := range dnIds {
			if dnId == deadDataNodeId {
				healthyDataNodeId := nameNode.BlockPathToDataNodeIds[blockPath][(i+1)%len(dnIds)]
				underReplicatedBlocksList = append(
					underReplicatedBlocksList,
					UnderReplicatedBlocks{blockPath, healthyDataNodeId},
				)
				delete(nameNode.BlockPathToDataNodeIds, blockPath)
				// TODO: trigger data deletion on the existing data nodes
				break
			}
		}
	}

	// verify if re-replication would be possible
	if len(nameNode.IdToDataNodes) < int(nameNode.ReplicationFactor) {
		log.Println("Replication not possible due to unavailability of sufficient DataNode(s)")
		return nil
	}

	var availableNodes []uint64
	for k := range nameNode.IdToDataNodes {
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
			BlockPathVar: blockToReplicate.BlockPathVar,
		}
		var getReply datanode.DataNodeData

		rpcErr = dataNodeInstance.Call("Service.GetData", getRequest, &getReply)
		util.Check(rpcErr)
		blockContents := getReply.Data

		// initiate the replication of the block contents
		targetDataNodeIds := nameNode.assignDataNodes(blockToReplicate.BlockPathVar, availableNodes, nameNode.ReplicationFactor)
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
			BlockPathVar:     blockToReplicate.BlockPathVar,
			Data:             blockContents,
			ReplicationNodes: remainingDataNodes,
		}
		var putReply datanode.DataNodeWriteStatus

		rpcErr = targetDataNodeInstance.Call("Service.PutData", putRequest, &putReply)
		util.Check(rpcErr)

		log.Printf("Block %s replication completed for %+v datanodes\n", blockToReplicate.BlockPathVar.BlockId, targetDataNodeIds)
	}

	return nil
}

func (nameNode *Service) Ls(request *NameNodeLsRequest, reply *string) error {
	dirPath := request.DirPath
	if dirPath == "" {
		return nil
	}

	for k, _ := range nameNode.ClientPathToBlockPath {
		fullPath := k.MachineName + k.SourcePath + k.FileName

		res := strings.HasPrefix(fullPath, dirPath)
		if res {
			suffix := strings.TrimPrefix(fullPath, dirPath)
			if dirPath[len(dirPath)-1:] == "/" {
				suffixes := strings.Split(suffix, "/")
				if !strings.Contains(*reply, suffix) {
					if len(suffixes) > 1 {
						*reply += suffixes[0] + "/ "
					} else {
						*reply += suffixes[0] + " "
					}
				}
			} else if suffix[:1] == "/" {
				suffixes := strings.Split(suffix[1:], "/")
				if !strings.Contains(*reply, suffix) {
					if len(suffixes) > 1 {
						*reply += suffixes[0] + "/ "
					} else {
						*reply += suffixes[0] + " "
					}
				}
			}
		}
	}

	return nil
}

func (nameNode *Service) Search(request *NameNodeSearchRequest, reply *string) error {
	clientPath := request.ClientPathVar.MachineName + request.ClientPathVar.SourcePath + request.ClientPathVar.FileName

	for k, _ := range nameNode.ClientPathToBlockPath {
		fullPath := k.MachineName + k.SourcePath + k.FileName

		if fullPath == clientPath {
			*reply = clientPath
			return nil
		} else {
			res := strings.HasPrefix(fullPath, clientPath)
			if res {
				suffix := strings.TrimPrefix(fullPath, clientPath)
				if clientPath[len(clientPath)-1:] == "/" {
					suffix = strings.Split(suffix, "/")[0]
					*reply = clientPath
					return nil
				} else if suffix[:1] == "/" {
					suffix = strings.Split(suffix[1:], "/")[0]
					*reply = clientPath
					return nil
				}
			}
		}
	}

	*reply = "No items found"
	return nil
}
