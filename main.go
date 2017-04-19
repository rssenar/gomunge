package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

type customer struct {
	id         int
	firstname  string
	mi         string
	lastname   string
	address1   string
	address2   string
	city       string
	state      string
	zip        string
	zip4       string
	hph        string
	bph        string
	cph        string
	email      string
	vin        string
	year       string
	make       string
	model      string
	deldate    time.Time
	date       time.Time
	dsfwalkseq string
	crrt       string
	kbb        string
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
