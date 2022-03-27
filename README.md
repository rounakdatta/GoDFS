## UFS
This is a simple implementation of a Distributed File System that uses replication.

## Usage
One NameNode and at least one DataNode(s) must be initiated as daemons through the command line interface provided.

The CLI can be compiled to a binary to obtain `ufs` as:
```bash
make build
```

### Natively

- **DataNode daemon**
	Syntax:
	```bash
	./ufs datanode --port <portNumber> --data-location <dataLocation>
	```
	Sample command:
	```bash
	./ufs datanode --port 7000 --data-location .dndata1/
	```

- **NameNode daemon**
	Syntax:
	```bash
	./ufs namenode --port <portNumber> --datanodes <dnEndpoints> --block-size <blockSize> --replication-factor <replicationFactor> 
	```
	Sample command:
	```bash
	./ufs namenode --datanodes localhost:7000,localhost:7001,localhost:7002 --block-size 10 --replication-factor 2
	```
	
- **Client**
Currently the following operations are supported:
	- **Put** operation
	Syntax:
		```bash
		./ufs client --namenode <nnEndpoint> --operation put --source-path <locationToFile> --filename <fileName>
		```
		Sample command:
		```bash
		./ufs client --namenode localhost:9000 --operation put --source-path ./ --filename File.txt
		```
	- **Get** operation
	Syntax:
		```bash
		./ufs client --namenode <nnEndpoint> --operation get --source-path <locationToFile> --filename <fileName>
		```
		Sample command:
		```bash
		./ufs client --namenode localhost:9000 --operation get --source-path <locationToFile> --filename File.txt
		```
	- **Putd** operation
	Syntax:
		```bash
		./ufs client --namenode <nnEndpoint> --operation putd --source-path <locationToFile>
		```
		Sample command:
		```bash
		./ufs client --namenode localhost:9000 --operation put --source-path ./Directory 
		```
	- **ls** operation
	Syntax:
		```bash
		./ufs client --namenode <nnEndpoint> --operation ls --source-path <locationToFile>
		```
		Sample command:
		```bash
		./ufs client --namenode localhost:9000 --operation ls --source-path ./Directory 
		```
	- **Search** operation
	Syntax:
		```bash
		./ufs client --namenode <nnEndpoint> --operation search --source-path <locationToFile> [--filename <fileName>]
		```
		Sample command:
		```bash
		./ufs client --namenode localhost:9000 --operation search --source-path ./Directory --filename File.txt
		```
