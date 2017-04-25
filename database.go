package main

import (
	"encoding/json"
	"log"

	"github.com/boltdb/bolt"
)

func initializeDB() *bolt.DB {
	db, err := bolt.Open("~/Dropbox/Resource/customer.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

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
