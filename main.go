package main

import (
	"sync"
	"time"
)

var (
	param = newDataInfo()
	wg    sync.WaitGroup
)

type customer struct {
	ID         int       `json:"id"`
	Firstname  string    `json:"first_name"`
	MI         string    `json:"middle_name"`
	Lastname   string    `json:"last_name"`
	Address1   string    `json:"address_1"`
	Address2   string    `json:"address_2"`
	City       string    `json:"city"`
	State      string    `json:"state"`
	Zip        string    `json:"zip"`
	Zip4       string    `json:"zip_4"`
	HPH        string    `json:"home_phone"`
	BPH        string    `json:"business_phone"`
	CPH        string    `json:"mobile_phone"`
	Email      string    `json:"email"`
	VIN        string    `json:"VIN"`
	Year       string    `json:"year"`
	Make       string    `json:"make"`
	Model      string    `json:"model"`
	DelDate    time.Time `json:"delivery_date"`
	Date       time.Time `json:"date"`
	DSFwalkseq string    `json:"DSF_Walk_Sequence"`
	CRRT       string    `json:"CRRT"`
	KBB        string    `json:"KBB"`
}

func main() {
	wg.Add(1000)
	go tasksFactory()
	go func() {
		wg.Wait()
		close(param.results)
	}()
	for i := 0; i < 1000; i++ {
		go processTasks()
	}
	outputCSV()
}
