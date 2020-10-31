## GoDFS
Very Simple Distributed FileSystem implementation on the lines of [GFS](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf) and [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.pdf)

### NameNode
The NameNode is the master and is the single point of contact (and failure!) for coordinating all operations. It provides the clients with all the required metadata for accessing the DataNodes.

### DataNode
The DataNode is a simple data containing unit and clients talk to them directly to put / get data into / from them.

### Client
The short-lived client does all the talking to get operations done. A read / write client can be initiated through the command line interface provided.

## Usage
One NameNode and at least one DataNode(s) must be initiated as daemons through the command line interface provided.

Run unit tests as:
```bash
make test
```

The CLI can be compiled to a binary to obtain `godfs` as:
```bash
make build
```

### Natively

- **DataNode daemon**
	Syntax:
	```bash
	./godfs datanode --port <portNumber> --data-location <dataLocation>
	```
	Sample command:
	```bash
	./godfs datanode --port 7002 --data-location .dndata3/
	```

- **NameNode daemon**
	Syntax:
	```bash
	./godfs namenode --port <portNumber> --datanodes <dnEndpoints> --block-size <blockSize> --replication-factor <replicationFactor> 
	```
	Sample command:
	```bash
	./godfs namenode --datanodes localhost:7000,localhost:7001,localhost:7002 --block-size 10 --replication-factor 2
	```
	
- **Client**
Currently Put and Get operations are supported
	- **Put** operation
	Syntax:
		```bash
		./godfs client --namenode <nnEndpoint> --operation put --source-path <locationToFile> --filename <fileName>
		```
		Sample command:
		```bash
		./godfs client --namenode localhost:9000 --operation put --source-path ./ --filename foo.bar
		```
	- **Get** operation
	Syntax:
		```bash
		./godfs client --namenode <nnEndpoint> --operation get --filename <fileName>
		```
		Sample command:
		```bash
		./godfs client --namenode localhost:9000 --operation get --filename foo.bar
		```

### Containerized through Docker Compose
- Build the images for the components:
    ```bash
     docker build -t datanode -f daemon/datanode/Dockerfile .
     docker build -t namenode -f daemon/namenode/Dockerfile .
     docker build -t client -f daemon/client/Dockerfile .
    ```
- Initiate the DataNode and the NameNode services (scale up accordingly):
    ```bash
    docker-compose up --scale datanode=6 --remove-orphans --force-recreate
    ```
- Start the client in a new container under the base host:
    ```bash
    docker run -it --network host client
    ```
 - Make file `put` and `get` requests using similar commands as above
	
## Todo
- [x] NameNode's regular HeartBeat check for DataNodes
- [x] Re-replication in case of dying DataNode
- [ ] Secondary NameNode
