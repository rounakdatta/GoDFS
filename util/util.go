package util

type DataNodeInstance struct {
	Host        string
	ServicePort string
}

// the path to the block on the DFS (regardless of data node)
type BlockPath struct {
	MachineName string
	SourcePath  string
	BlockId     string
}

// the path to the block on the corresponding data node
type BlockNodePath struct {
	DataDirectory string
	BlockPathVar  BlockPath
}

// the path to the file on the local machine
// includes the name of the machine for future assignment
// to a BlockPath object
type ClientPath struct {
	MachineName string
	SourcePath  string
	FileName    string
}

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func CheckStatus(e bool) {
	if !e {
		panic(e)
	}
}
