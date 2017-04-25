package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blendlabs/go-name-parser"
)

type dataInfo struct {
	columns map[string]int
	dupes   map[string]int
	VIN     map[string]int
	tasks   chan []string
	results chan *customer
	output  chan *customer
}

func newDataInfo() *dataInfo {
	return &dataInfo{
		columns: make(map[string]int),
		dupes:   make(map[string]int),
		VIN:     make(map[string]int),
		tasks:   make(chan []string),
		results: make(chan *customer),
		output:  make(chan *customer),
	}
}

func (c *dataInfo) setColumns(record []string) {
	for idx, value := range record {
		switch {
		case regexp.MustCompile(`(?i)ful.+me`).MatchString(value):
			c.columns["fullname"] = idx
		case regexp.MustCompile(`(?i)fir.+me`).MatchString(value):
			c.columns["firstname"] = idx
		case regexp.MustCompile(`(?i)^mi$`).MatchString(value):
			c.columns["mi"] = idx
		case regexp.MustCompile(`(?i)las.+me`).MatchString(value):
			c.columns["lastname"] = idx
		case regexp.MustCompile(`(?i)addr.+1`).MatchString(value):
			c.columns["address1"] = idx
		case regexp.MustCompile(`(?i)addr.+2`).MatchString(value):
			c.columns["address2"] = idx
		case regexp.MustCompile(`(?i)^city$`).MatchString(value):
			c.columns["city"] = idx
		case regexp.MustCompile(`(?i)^state$`).MatchString(value):
			c.columns["state"] = idx
		case regexp.MustCompile(`(?i)^zip$`).MatchString(value):
			c.columns["zip"] = idx
		case regexp.MustCompile(`(?i)^zip4$`).MatchString(value):
			c.columns["zip4"] = idx
		case regexp.MustCompile(`(?i)^hph$`).MatchString(value):
			c.columns["hph"] = idx
		case regexp.MustCompile(`(?i)^bph$`).MatchString(value):
			c.columns["bph"] = idx
		case regexp.MustCompile(`(?i)^cph$`).MatchString(value):
			c.columns["cph"] = idx
		case regexp.MustCompile(`(?i)^email$`).MatchString(value):
			c.columns["email"] = idx
		case regexp.MustCompile(`(?i)^vin$`).MatchString(value):
			c.columns["vin"] = idx
		case regexp.MustCompile(`(?i)^year$`).MatchString(value):
			c.columns["year"] = idx
		case regexp.MustCompile(`(?i)^vyr$`).MatchString(value):
			c.columns["year"] = idx
		case regexp.MustCompile(`(?i)^make$`).MatchString(value):
			c.columns["make"] = idx
		case regexp.MustCompile(`(?i)^vmk$`).MatchString(value):
			c.columns["make"] = idx
		case regexp.MustCompile(`(?i)^model$`).MatchString(value):
			c.columns["model"] = idx
		case regexp.MustCompile(`(?i)^vmd$`).MatchString(value):
			c.columns["model"] = idx
		case regexp.MustCompile(`(?i)^deldate$`).MatchString(value):
			c.columns["deldate"] = idx
		case regexp.MustCompile(`(?i)^date$`).MatchString(value):
			c.columns["date"] = idx
		case regexp.MustCompile(`(?i)^DSF_WALK_SEQ$`).MatchString(value):
			c.columns["dsfwalkseq"] = idx
		case regexp.MustCompile(`(?i)^Crrt$`).MatchString(value):
			c.columns["crrt"] = idx
		case regexp.MustCompile(`(?i)^KBB$`).MatchString(value):
			c.columns["kbb"] = idx
		}
	}
}

func (c *dataInfo) process(record []string) *customer {
	customer := &customer{}
	for header := range c.columns {
		switch header {
		case "fullname":
			if customer.Firstname == "" || customer.Lastname == "" {
				name := names.Parse(record[c.columns[header]])
				customer.Firstname = tCase(name.FirstName)
				customer.MI = tCase(name.MiddleName)
				customer.Lastname = tCase(name.LastName)
			}
		case "firstname":
			if customer.Firstname == "" {
				customer.Firstname = tCase(record[c.columns[header]])
			}
		case "mi":
			if customer.MI == "" {
				customer.MI = tCase(record[c.columns[header]])
			}
		case "lastname":
			if customer.Lastname == "" {
				customer.Lastname = tCase(record[c.columns[header]])
			}
		case "address1":
			customer.Address1 = tCase(record[c.columns[header]])
		case "address2":
			customer.Address2 = tCase(record[c.columns[header]])
		case "city":
			customer.City = tCase(record[c.columns[header]])
		case "state":
			customer.State = uCase(record[c.columns[header]])
		case "zip":
			customer.Zip, _ = parseZip(record[c.columns[header]])
		case "zip4":
			_, customer.Zip4 = parseZip(record[c.columns[header]])
		case "hph":
			customer.HPH = parsePhone(record[c.columns[header]])
		case "bph":
			customer.BPH = parsePhone(record[c.columns[header]])
		case "cph":
			customer.CPH = parsePhone(record[c.columns[header]])
		case "email":
			customer.Email = lCase(record[c.columns[header]])
		case "vin":
			customer.VIN = uCase(record[c.columns[header]])
		case "year":
			if _, err := strconv.Atoi(record[c.columns[header]]); err == nil {
				customer.Year = decodeYr(record[c.columns[header]])
			}
		case "make":
			customer.Make = tCase(record[c.columns[header]])
		case "model":
			customer.Model = tCase(record[c.columns[header]])
		case "deldate":
			customer.DelDate = parseDate(record[c.columns[header]])
		case "date":
			customer.Date = parseDate(record[c.columns[header]])
		case "dsfwalkseq":
			if _, err := strconv.Atoi(record[c.columns[header]]); err == nil {
				customer.DSFwalkseq = record[c.columns[header]]
			}
		case "crrt":
			customer.CRRT = uCase(record[c.columns[header]])
		case "kbb":
			if _, err := strconv.Atoi(record[c.columns[header]]); err == nil {
				customer.KBB = record[c.columns[header]]
			}
		}
	}
	return customer
}

func tCase(f string) string {
	return strings.TrimSpace(strings.Title(strings.ToLower(f)))
}

func uCase(f string) string {
	return strings.TrimSpace(strings.ToUpper(f))
}

func lCase(f string) string {
	return strings.TrimSpace(strings.ToLower(f))
}

func parseZip(zip string) (string, string) {
	switch {
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9]$`).MatchString(zip):
		return zip, ""
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9][0-9]$`).MatchString(zip):
		if zip[:1] == "0" {
			zip = zip[1:]
			return zip, ""
		}
		return zip, ""
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$`).MatchString(zip):
		return zip[:5], zip[5:]
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]$`).MatchString(zip):
		zip := strings.Split(zip, "-")
		return zip[0], zip[1]
	}
	return "", ""
}

func parseDate(d string) time.Time {
	if d != "" {
		formats := []string{"1/2/2006", "1-2-2006", "1/2/06", "1-2-06",
			"2006/1/2", "2006-1-2"}
		for _, f := range formats {
			if date, err := time.Parse(f, d); err == nil {
				return date
			}
		}
	}
	return time.Time{}
}

func parsePhone(p string) string {
	sep := []string{"-", ".", "*", "(", ")", " "}
	for _, v := range sep {
		p = strings.Replace(p, v, "", -1)
	}
	switch len(p) {
	case 10:
		return fmt.Sprintf("(%v) %v-%v", p[0:3], p[3:6], p[6:10])
	case 7:
		return fmt.Sprintf("%v-%v", p[0:3], p[3:7])
	default:
		return ""
	}
}

func taskGenerator(param *dataInfo) {
	file, err := os.Open(fmt.Sprintf("./%v.csv", fileName))
	if err != nil {
		log.Fatalln(err)
	}
	reader := csv.NewReader(file)
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
}

func process(param *dataInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range param.tasks {
		param.results <- param.process(task)
	}
}

func output() (func(x *customer), *os.File) {
	file, err := os.Create(fmt.Sprintf("./%v_OUTPUT.csv", fileName))
	if err != nil {
		log.Fatalln(err)
	}
	writer := csv.NewWriter(file)
	header := []string{
		"Key_Head",
		"Key_Counter",
		"Customer ID",
		"FirstName",
		"MI",
		"LastName",
		"Address1",
		"City",
		"State",
		"Zip",
		"HPH",
		"BPH",
		"CPH",
		"VIN",
		"Year",
		"Make",
		"Model",
		"DelDate",
		"Date",
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
	counter := genSeqNum()
	return func(x *customer) {
		ctr := counter
		newwriter := writer
		var r []string
		r = append(r, fmt.Sprintf("%v", x.pk.Head))
		r = append(r, fmt.Sprintf("%v", x.pk.Counter))
		r = append(r, fmt.Sprintf("%v%v", source, ctr()+100000))
		r = append(r, x.Firstname)
		r = append(r, x.MI)
		r = append(r, x.Lastname)
		r = append(r, fmt.Sprintf("%v %v", x.Address1, x.Address2))
		r = append(r, x.City)
		r = append(r, x.State)
		r = append(r, x.Zip)
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
		newwriter.Write(r)
		newwriter.Flush()
	}, file
}

func errStatus() (func(x *customer), *os.File) {
	file, err := os.Create(fmt.Sprintf("./%v_DUPES.csv", fileName))
	if err != nil {
		log.Fatalln(err)
	}
	writer := csv.NewWriter(file)
	header := []string{
		"Seq#",
		"FirstName",
		"LastName",
		"Address",
		"City",
		"State",
		"Zip",
		"VIN",
		"Status",
	}
	writer.Write(header)
	writer.Flush()
	counter := genSeqNum()
	return func(x *customer) {
		ctr := counter
		newwriter := writer
		var r []string
		r = append(r, fmt.Sprintf("%v", ctr()))
		r = append(r, x.Firstname)
		r = append(r, x.Lastname)
		r = append(r, fmt.Sprintf("%v %v", x.Address1, x.Address2))
		r = append(r, x.City)
		r = append(r, x.State)
		r = append(r, x.Zip)
		r = append(r, x.VIN)
		r = append(r, x.ErrStat)
		newwriter.Write(r)
		newwriter.Flush()
	}, file
}

func outputPhones() (func(x *customer), *os.File) {
	file, err := os.Create(fmt.Sprintf("./%v_PHONES.csv", fileName))
	if err != nil {
		log.Fatalln(err)
	}
	writer := csv.NewWriter(file)
	header := []string{
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
	counter := genSeqNum()
	return func(x *customer) {
		ctr := counter
		newwriter := writer
		var r []string
		if x.HPH != "" {
			r = append(r, fmt.Sprintf("%v", ctr()))
			r = append(r, x.Firstname)
			r = append(r, x.Lastname)
			r = append(r, fmt.Sprintf("%v %v", x.Address1, x.Address2))
			r = append(r, x.City)
			r = append(r, x.State)
			r = append(r, x.Zip)
			r = append(r, x.HPH)
			newwriter.Write(r)
			newwriter.Flush()
		}
	}, file
}

func comb(cust *customer) string {
	return fmt.Sprintf("%v %v %v", cust.Address1, cust.Address2, cust.Zip)
}

func genSeqNum() func() int {
	i := 0
	return func() int {
		i++
		return i
	}
}

func genPrimaryKeyCounter() func() int {
	i := 100000000
	return func() int {
		i++
		return i
	}
}
