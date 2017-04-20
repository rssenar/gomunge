package main

import (
	"encoding/csv"
	"fmt"
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
	cust := newDataInfo()
	csvReader := csv.NewReader(os.Stdin)
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
			record = append(record, customer.MI)
			record = append(record, customer.Lastname)
			record = append(record, fmt.Sprintf("%v %v", customer.Address1, customer.Address2))
			record = append(record, customer.City)
			record = append(record, customer.State)
			record = append(record, customer.Zip)
			record = append(record, customer.Zip4)
			record = append(record, customer.HPH)
			record = append(record, customer.BPH)
			record = append(record, customer.CPH)
			record = append(record, customer.Email)
			record = append(record, customer.VIN)
			record = append(record, customer.Year)
			record = append(record, customer.Make)
			record = append(record, customer.Model)
			record = append(record, customer.DelDate.String())
			record = append(record, customer.Date.String())
			record = append(record, customer.DSFwalkseq)
			record = append(record, customer.CRRT)
			record = append(record, customer.KBB)

			writer := csv.NewWriter(os.Stdout)
			writer.Write(record)
			writer.Flush()
		}
	}
}
