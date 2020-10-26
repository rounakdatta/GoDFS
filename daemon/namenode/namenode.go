package namenode

import (
	"log"
	"net"
	"net/rpc"
	"strconv"

	"github.com/rounakdatta/GoDFS/namenode"
	"github.com/rounakdatta/GoDFS/datanode"
	"github.com/rounakdatta/GoDFS/util"
)

func discoverDataNodes(nameNodeInstance *namenode.Service, listOfDataNodes []string) error {
	nameNodeInstance.IdToDataNodes = make(map[uint64]util.DataNodeInstance)

	var i int
	availableNumberOfDataNodes := len(listOfDataNodes)
	if availableNumberOfDataNodes == 0 {
		log.Printf("No DataNodes specified, discovering ...\n")

		host := "localhost"
		serverPort := 7000

		pingRequest := datanode.NameNodePingRequest{Host: host, Port: nameNodeInstance.Port}
		var pingResponse datanode.NameNodePingResponse

		for serverPort < 7050 {
			dataNodeUri := host + ":" + strconv.Itoa(serverPort)
			dataNodeInstance, initErr := rpc.Dial("tcp", dataNodeUri)
			if initErr == nil {
				listOfDataNodes = append(listOfDataNodes, dataNodeUri)
				log.Printf("Discovered DataNode %s\n", dataNodeUri)

				pingErr := dataNodeInstance.Call("Service.PingToDataNode", pingRequest, &pingResponse)
				util.Check(pingErr)
				if pingResponse.Ack {
					log.Printf("Ack received from %s\n", dataNodeUri)
				} else {
					log.Printf("No ack received from %s\n", dataNodeUri)
				}

				serverPort += 1
			}
		}
	}

	availableNumberOfDataNodes = len(listOfDataNodes)
	for i = 0; i < availableNumberOfDataNodes; i++ {
		host, port, err := net.SplitHostPort(listOfDataNodes[i])
		util.Check(err)
		dataNodeInstance := util.DataNodeInstance{Host: host, ServicePort: port}
		nameNodeInstance.IdToDataNodes[uint64(i)] = dataNodeInstance
	}

	return nil
}

func InitializeNameNodeUtil(serverPort int, blockSize int, replicationFactor int, listOfDataNodes []string) {
	nameNodeInstance := namenode.NewService(uint64(blockSize), uint64(replicationFactor), uint16(serverPort))
	err := discoverDataNodes(nameNodeInstance, listOfDataNodes)
	util.Check(err)

	log.Printf("BlockSize is %d\n", blockSize)
	log.Printf("Replication Factor is %d\n", replicationFactor)
	log.Printf("List of DataNode(s) in service is %q\n", listOfDataNodes)
	log.Printf("NameNode port is %d\n", serverPort)

	err = rpc.Register(nameNodeInstance)
	util.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(serverPort))
	util.Check(err)
	defer listener.Close()

	rpc.Accept(listener)

	log.Println("NameNode daemon started on port: " + strconv.Itoa(serverPort))
}
