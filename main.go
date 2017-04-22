package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"sync"
	"time"
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

var param = newDataInfo()

func main() {
	var wg sync.WaitGroup

	timer := time.Now()
	log.Println("Processing Data...")

	go func() {
		reader := csv.NewReader(os.Stdin)
		for ctr := 0; ; ctr++ {
			rec, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("Error processing CSV file: %v \n", err)
			}
			if ctr == 0 {
				param.setColumns(rec)
			} else {
				param.tasks <- rec
			}
		}
		close(param.tasks)
	}()

	go func() {
		wg.Wait()
		close(param.results)
	}()

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			for t := range param.tasks {
				param.results <- param.parseColumns(t)
			}
		}()
	}

	outputCSV()
	log.Printf("Completed in %v", time.Since(timer))
}
