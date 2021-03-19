package namenode

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"

	"github.com/rounakdatta/GoDFS/namenode"
)

func SerializeNameNodeImage(image *namenode.Service) (string, error) {
	buf := bytes.Buffer{}
	e := gob.NewEncoder(&buf)
	err := e.Encode(image)

	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func DeserializeNameNodeImage(serializedImage string) (*namenode.Service, error) {
	image := namenode.Service{}

	data, err := base64.StdEncoding.DecodeString(serializedImage)
	if err != nil {
		return nil, err
	}

	buf := bytes.Buffer{}
	buf.Write(data)
	d := gob.NewDecoder(&buf)
	err = d.Decode(&image)

	if err != nil {
		return nil, err
	}

	return &image, nil
}
