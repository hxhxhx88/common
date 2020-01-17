package io

import (
	"io/ioutil"

	"github.com/golang/protobuf/proto"
)

// LoadPB ...
func LoadPB(filePath string, dataPtr proto.Message) (err error) {
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return
	}

	if err = proto.Unmarshal(bytes, dataPtr); err != nil {
		return
	}
	return
}

// SavePB ...
func SavePB(filePath string, data proto.Message) (err error) {
	bytes, err := proto.Marshal(data)
	if err != nil {
		return
	}
	if err = ioutil.WriteFile(filePath, bytes, 0644); err != nil {
		return
	}
	return
}
