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

func main() {
	// Open CSV file
	f, err := os.Open("test.csv")
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	// Open Output json file
	outfile, err := os.Open("/Users/richardsenar/desktop/test.csv")
	if err != nil {
		log.Fatalln(err)
	}
	defer outfile.Close()

	cust := newColumnInfo()

	csvReader := csv.NewReader(f)
	for rowCount := 0; ; rowCount++ {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalln(err)
		}
		if rowCount == 0 {
			cust.setColumns(record)
		} else {
			customer := cust.parseColumns(record, rowCount)
			_ = customer
		}
	}
}
