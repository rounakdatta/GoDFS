package namenode

import (
	"bufio"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"

	"github.com/rounakdatta/GoDFS/datanode"
	"github.com/rounakdatta/GoDFS/namenode"
	"github.com/rounakdatta/GoDFS/util"
)

const NameNodeState = "NN.STATE"

func removeElementFromSlice(elements []string, index int) []string {
	return append(elements[:index], elements[index+1:]...)
}

func discoverDataNodes(nameNodeInstance *namenode.Service, listOfDataNodes *[]string) error {
	nameNodeInstance.IdToDataNodes = make(map[uint64]util.DataNodeInstance)

	var i int
	availableNumberOfDataNodes := len(*listOfDataNodes)
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
				*listOfDataNodes = append(*listOfDataNodes, dataNodeUri)
				log.Printf("Discovered DataNode %s\n", dataNodeUri)

				pingErr := dataNodeInstance.Call("Service.Ping", pingRequest, &pingResponse)
				util.Check(pingErr)
				if pingResponse.Ack {
					log.Printf("Ack received from %s\n", dataNodeUri)
				} else {
					log.Printf("No ack received from %s\n", dataNodeUri)
				}
			}
			serverPort += 1
		}

	}

	availableNumberOfDataNodes = len(*listOfDataNodes)
	for i = 0; i < availableNumberOfDataNodes; i++ {
		host, port, err := net.SplitHostPort((*listOfDataNodes)[i])
		util.Check(err)
		dataNodeInstance := util.DataNodeInstance{Host: host, ServicePort: port}
		nameNodeInstance.IdToDataNodes[uint64(i)] = dataNodeInstance
	}

	return nil
}

func InitializeNameNodeUtil(serverPort int, blockSize int, replicationFactor int, isPrimary bool, listOfDataNodes []string) {
	nameNodeInstance := namenode.NewService(uint64(blockSize), uint64(replicationFactor), uint16(serverPort))
	err := discoverDataNodes(nameNodeInstance, &listOfDataNodes)
	util.Check(err)

	log.Printf("BlockSize is %d\n", blockSize)
	log.Printf("Replication Factor is %d\n", replicationFactor)
	log.Printf("List of DataNode(s) in service is %q\n", listOfDataNodes)
	log.Printf("NameNode port is %d\n", serverPort)

	if isPrimary {
		log.Printf("Starting processes for primary NameNode\n")
		go heartbeatToDataNodes(listOfDataNodes, nameNodeInstance)
		go scheduledFlushToDisk(nameNodeInstance)
	} else {
		log.Printf("Starting processes for secondary NameNode\n")
		go getServiceStateFromPrimary(nameNodeInstance, listOfDataNodes)
	}

	err = rpc.Register(nameNodeInstance)
	util.Check(err)

	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(serverPort))
	util.Check(err)
	defer listener.Close()

	rpc.Accept(listener)

	log.Println("NameNode daemon started on port: " + strconv.Itoa(serverPort))
}

func heartbeatToDataNodes(listOfDataNodes []string, nameNode *namenode.Service) {
	for range time.Tick(time.Second * 5) {
		for i, hostPort := range listOfDataNodes {
			nameNodeClient, connectionErr := rpc.Dial("tcp", hostPort)

			if connectionErr != nil {
				log.Printf("Unable to connect to node %s\n", hostPort)
				var reply bool
				reDistributeError := nameNode.ReDistributeData(&namenode.ReDistributeDataRequest{DataNodeUri: hostPort}, &reply)
				util.Check(reDistributeError)
				delete(nameNode.IdToDataNodes, uint64(i))
				listOfDataNodes = removeElementFromSlice(listOfDataNodes, i)
				continue
			}

			var response bool
			hbErr := nameNodeClient.Call("Service.Heartbeat", true, &response)
			if hbErr != nil || !response {
				log.Printf("No heartbeat received from %s\n", hostPort)
				var reply bool
				reDistributeError := nameNode.ReDistributeData(&namenode.ReDistributeDataRequest{DataNodeUri: hostPort}, &reply)
				util.Check(reDistributeError)
				delete(nameNode.IdToDataNodes, uint64(i))
				listOfDataNodes = removeElementFromSlice(listOfDataNodes, i)
			}
		}
	}
}

func scheduledFlushToDisk(namenode *namenode.Service) {
	for range time.Tick(time.Second * 10) {
		flushServiceStateToDisk(namenode)
	}
}

func flushServiceStateToDisk(namenode *namenode.Service) {
	state, err := SerializeNameNodeImage(namenode)
	util.Check(err)

	fileWriteHandler, err := os.Create(NameNodeState)
	util.Check(err)

	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(state)
	util.Check(err)
	fileWriter.Flush()
	fileWriteHandler.Close()
	log.Println("NameNodeService state flushed to disk")
}

func getServiceStateFromPrimary(namenodeInstance *namenode.Service, listOfDataNodes []string) {
	primaryNameNodeHost := "localhost"
	const primaryNameNodePort = "9000"

	for range time.Tick(time.Second * 2) {
		primaryNameNodeClient, connectionErr := rpc.Dial("tcp", primaryNameNodeHost+":"+primaryNameNodePort)

		if connectionErr != nil {
			log.Printf("Unable to connect to NameNode on %s, starting recovery\n", primaryNameNodePort)
			dataBytes, err := ioutil.ReadFile(NameNodeState)
			util.Check(err)

			namenodeInstance, err = DeserializeNameNodeImage(string(dataBytes))
			util.Check(err)
			go heartbeatToDataNodes(listOfDataNodes, namenodeInstance)
			go flushServiceStateToDisk(namenodeInstance)
			return
		}

		var response *namenode.Service
		stateFetchErr := primaryNameNodeClient.Call("Service.GetState", true, &response)
		if stateFetchErr != nil {
			log.Println("Error fetching state from primary NameNode")
		}

		flushServiceStateToDisk(response)
	}
}
