package main

import "encoding/json"

func marshal(pointerToData interface{}) ([]byte, error) {
	buff, err := json.Marshal(pointerToData)
	if err != nil {
		return nil, err
	}
	return buff, nil
}

func unmarshal(pointerToData interface{}, bs []byte) error {
	err := json.Unmarshal(bs, &pointerToData)
	if err != nil {
		return err
	}
	return nil
}
