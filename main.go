package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
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
	f, err := os.Open("test.csv")
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	cust := newDataInfo()

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
			if customer, err = cust.deDupe(customer); err != nil {
				continue
			}

			var record []string
			record = append(record, strconv.Itoa(customer.ID))
			record = append(record, customer.Firstname)
			record = append(record, customer.Lastname)
			record = append(record, customer.Address1)
			record = append(record, customer.Address2)
			record = append(record, customer.City)
			record = append(record, customer.State)
			record = append(record, customer.Zip)
			writer := csv.NewWriter(os.Stdout)
			writer.Write(record)
			writer.Flush()
		}
	}
}
