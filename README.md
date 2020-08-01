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

- **DataNode daemon**
	Syntax:
	```bash
	./godfs datanode --port <portNumber> --data-location <dirLocation>
	```
	Sample command:
	```bash
	./godfs datanode --data-location .dndata/ 
	```

- **NameNode daemon**
	Syntax:
	```bash
	./godfs namenode --port <portNumber> --list-datanodes <commaSepUris> <blockSize> <replicationFactor> 
	```
	Sample command:
	```bash
	./godfs namenode --list-datanodes localhost:7000,localhost:7001 10 1  
	```
	
- **Client**
Currently Put and Get operations are supported
	- **Put** operation
	Syntax:
		```bash
		./godfs client --namenode-address <nnPort> --operation put <locationToFile> <fileName>
		```
		Sample command:
		```bash
		./godfs client --namenode-address 9000 --operation put ./ foo.txt
		```
	- **Get** operation
	Syntax:
		```bash
		./godfs client --namenode-address <nnPort> --operation get <fileName>
		```
		Sample command:
		```bash
		./godfs client --namenode-address 9000 --operation get foo.txt  
		```
	
## Todo
- [ ] DataNodes send heartbeat to NameNode
- [ ] Secondary NameNode
