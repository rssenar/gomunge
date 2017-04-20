package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"time"
)

// Customer struct defines customer header fields
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

var param *dataInfo

func main() {
	param = newDataInfo()
	//tasks := newTasks()
}

func newTasks() <-chan []string {
	t := make(chan []string)
	reader := csv.NewReader(os.Stdin)
	var ctr int
	for ctr = 0; ; ctr++ {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Error parsing CSV file: %v \n", err)
		}
		if ctr == 0 {
			param.setColumns(rec)
		} else {
			t <- rec
		}
	}
	close(t)
	log.Printf("Import complete, %v records")
	return t
}
