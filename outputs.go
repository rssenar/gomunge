package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func (d *dataInfo) custID(id int) string {
	var i int
	switch tCase(d.config.Source) {
	case "Database":
		i = id + 10000
	case "Purchase":
		i = id + 50000
	default:
		log.Fatalln("Invalid SOURCE: Needs to be [Database] or [Purchase]")
	}
	if i > 99999 {
		log.Fatalln("WARNING! CustID is > 99,999")
	}
	return strconv.Itoa(i)
}

func (d *dataInfo) outputCSV(cust []*customer) {
	file, err := os.Create(fmt.Sprintf("./%v_OUTPUT.csv", fileName))
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	header := []string{
		"Customer ID",
		"FirstName",
		"MI",
		"LastName",
		"Address",
		"City",
		"State",
		"Zip",
		"Zip4",
		"HPH",
		"BPH",
		"CPH",
		"VIN",
		"Year",
		"Make",
		"Model",
		"DelDate",
		"Date",
		"Radius",
		"Coordinates",
		"DSF_WALK_SEQ",
		"CRRT",
		"KBB",
		"DD_Year",
		"DD_Month",
		"D_Year",
		"D_Month",
	}
	writer.Write(header)
	writer.Flush()

	for idx, x := range cust {
		var r []string
		r = append(r, d.custID(idx))
		r = append(r, x.Firstname)
		r = append(r, x.MI)
		r = append(r, x.Lastname)
		r = append(r, x.combAddr(x))
		r = append(r, x.City)
		r = append(r, x.State)
		r = append(r, x.Zip)
		r = append(r, x.Zip4)
		r = append(r, x.HPH)
		r = append(r, x.BPH)
		r = append(r, x.CPH)
		r = append(r, x.VIN)
		r = append(r, x.Year)
		r = append(r, x.Make)
		r = append(r, x.Model)
		if !x.DelDate.IsZero() {
			r = append(r, x.DelDate.Format(time.RFC3339))
		} else {
			r = append(r, "")
		}
		if !x.Date.IsZero() {
			r = append(r, x.Date.Format(time.RFC3339))
		} else {
			r = append(r, "")
		}
		r = append(r, x.Radius)
		r = append(r, x.Coordinates)
		r = append(r, x.DSFwalkseq)
		r = append(r, x.CRRT)
		r = append(r, x.KBB)
		if !x.DelDate.IsZero() {
			r = append(r, strconv.Itoa(x.DelDate.Year()))
		} else {
			r = append(r, "")
		}
		if !x.DelDate.IsZero() {
			r = append(r, strconv.Itoa(int(x.DelDate.Month())))
		} else {
			r = append(r, "")
		}
		if !x.Date.IsZero() {
			r = append(r, strconv.Itoa(x.Date.Year()))
		} else {
			r = append(r, "")
		}
		if !x.Date.IsZero() {
			r = append(r, strconv.Itoa(int(x.Date.Month())))
		} else {
			r = append(r, "")
		}
		writer.Write(r)
		writer.Flush()
	}
}

func (d *dataInfo) errStatusCSV(cust []*customer) {
	file, err := os.Create(fmt.Sprintf("./%v_ERR.csv", fileName))
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	header := []string{
		"#",
		"FirstName",
		"LastName",
		"Address",
		"City",
		"State",
		"Zip",
		"VIN",
		"Year",
		"Make",
		"Model",
		"Status",
	}
	writer.Write(header)
	writer.Flush()

	for idx, x := range cust {
		var r []string
		r = append(r, fmt.Sprintf("%v", idx+1))
		r = append(r, x.Firstname)
		r = append(r, x.Lastname)
		r = append(r, x.combAddr(x))
		r = append(r, x.City)
		r = append(r, x.State)
		r = append(r, x.Zip)
		r = append(r, x.VIN)
		r = append(r, x.Year)
		r = append(r, x.Make)
		r = append(r, x.Model)
		r = append(r, x.ErrStat)
		writer.Write(r)
		writer.Flush()
	}
}

func (d *dataInfo) phonesCSV(cust []*customer) {
	file, err := os.Create(fmt.Sprintf("./%v_PHONES.csv", fileName))
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	header := []string{
		"#",
		"First Name",
		"Last Name",
		"Address",
		"City",
		"State",
		"Zip",
		"Home Phone",
	}
	writer.Write(header)
	writer.Flush()

	for idx, x := range cust {
		var r []string
		if x.HPH != "" {
			r = append(r, fmt.Sprintf("%v", idx+1))
			r = append(r, x.Firstname)
			r = append(r, x.Lastname)
			r = append(r, fmt.Sprintf("%v %v", x.Address1, x.Address2))
			r = append(r, x.City)
			r = append(r, x.State)
			r = append(r, x.Zip)
			r = append(r, x.HPH)
			writer.Write(r)
			writer.Flush()
		}
	}
}
