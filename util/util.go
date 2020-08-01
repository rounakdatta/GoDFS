package util

type DataNodeInstance struct {
	Host        string
	ServicePort string
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
