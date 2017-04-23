package main

import (
	"encoding/csv"
	"fmt"
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

func main() {
	timer := time.Now()
	param := newDataInfo()
	fmt.Println("Processing Data...")

	var wg sync.WaitGroup

	go func() {
		wg.Wait()
		close(param.results)
	}()

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

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			for task := range param.tasks {
				param.results <- param.process(task)
			}
		}()
	}

	go func() {
		for r := range param.results {
			addr := fmt.Sprintf("%v %v %v", r.Address1, r.Address2, r.Zip)
			if _, ok := param.dupes[addr]; !ok {
				param.dupes[addr]++
				param.output <- r
			} else {
				log.Printf("DUPLICATE RECORDS : %v %v %v %v %v %v %v\n", r.Firstname, r.Lastname, r.Address1, r.Address2, r.City, r.State, r.Zip)
			}
		}
		close(param.output)
	}()

	output(param.output)
	fmt.Printf("Completed in %v\n", time.Since(timer))
}
