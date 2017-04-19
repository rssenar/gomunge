package main

import (
	"encoding/csv"
	"fmt"
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
	f, err := os.Open("test.csv")
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

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
			fmt.Println("ID :", customer.id)
			fmt.Println("First :", customer.firstname)
			fmt.Println("MI :", customer.mi)
			fmt.Println("Last :", customer.lastname)
			fmt.Println("Address1 :", customer.address1)
			fmt.Println("Address2 :", customer.address2)
			fmt.Println("City :", customer.city)
			fmt.Println("State :", customer.state)
			fmt.Println("Zip :", customer.zip)
			fmt.Println("Zip4 :", customer.zip4)
			fmt.Println("Phone :", customer.hph)
			fmt.Println("Work Phone :", customer.bph)
			fmt.Println("Cell Phone :", customer.cph)
			fmt.Println("Email :", customer.email)
			fmt.Println("VIN :", customer.vin)
			fmt.Println("Year :", customer.year)
			fmt.Println("Make :", customer.make)
			fmt.Println("Model :", customer.model)
			fmt.Println("Delivery Date :", customer.deldate)
			fmt.Println("Serv Date :", customer.date)
			fmt.Println("Walk Seq :", customer.dsfwalkseq)
			fmt.Println("CRTT :", customer.crrt)
			fmt.Println("KBB :", customer.kbb)
			fmt.Println("")
			fmt.Println("")
		}
	}
}
