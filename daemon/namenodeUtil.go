package daemon

import (
	"github.com/rounakdatta/GoDFS/namenode"
	"github.com/rounakdatta/GoDFS/util"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

func discoverDataNodes(nameNodeInstance *namenode.Service, listOfDataNodes []string) error {
	nameNodeInstance.IdToDataNodes = make(map[uint64]util.DataNodeInstance)

	var i uint64
	availableNumberOfDataNodes := uint64(len(listOfDataNodes))
	for i = 0; i < availableNumberOfDataNodes; i++ {
		host, port, err := net.SplitHostPort(listOfDataNodes[i])
		util.Check(err)
		dataNodeInstance := util.DataNodeInstance{Host: host, ServicePort: port}
		nameNodeInstance.IdToDataNodes[i] = dataNodeInstance
	}

	return nil
}

func InitializeNameNodeUtil(serverPort int, blockSize string, replicationFactor string, listOfDataNodes []string) {
	blockSizeNum, err := strconv.Atoi(blockSize)
	util.Check(err)
	replicationFactorNum, err := strconv.Atoi(replicationFactor)
	util.Check(err)

	nameNodeInstance := namenode.NewService(uint64(blockSizeNum), uint64(replicationFactorNum))
	err = discoverDataNodes(nameNodeInstance, listOfDataNodes)
	util.Check(err)

	log.Printf("BlockSize is %d\n", blockSizeNum)
	log.Printf("Replication Factor is %d\n", replicationFactorNum)
	log.Printf("List of DataNode(s) in service is %q\n", listOfDataNodes)
	log.Printf("NameNode port is %d\n", serverPort)

	err = rpc.Register(nameNodeInstance)
	util.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":" + strconv.Itoa(serverPort))
	util.Check(err)
	defer listener.Close()

	rpc.Accept(listener)

	log.Println("DataNode daemon started on port: " + strconv.Itoa(serverPort))
}
