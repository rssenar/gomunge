package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blendlabs/go-name-parser"
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
	Radius     string    `json:"radius"`
	DSFwalkseq string    `json:"DSF_Walk_Sequence"`
	CRRT       string    `json:"CRRT"`
	KBB        string    `json:"KBB"`
	ErrStat    string    `json:"Status"`
}

type dataInfo struct {
	config      *initConfig
	columns     map[string]int
	dupes       map[string]int
	VIN         map[string]int
	coordinates map[string][]string
	SCFFac      map[string]string
	DDUFac      map[string]string
	Ethnicity   map[string]int
	DNM         map[string]int
	GenSupp     map[string]int
	tasks       chan []string
	results     chan *customer
}

func newDataInfo() *dataInfo {
	return &dataInfo{
		config:      loadConfig(),
		coordinates: loadZipCor(),
		SCFFac:      loadSCFFac(),
		DDUFac:      loadDDUFac(),
		Ethnicity:   loadEthnicity(),
		DNM:         loadDNM(),
		GenSupp:     loadGenS(),
		columns:     make(map[string]int),
		dupes:       make(map[string]int),
		VIN:         make(map[string]int),
		tasks:       make(chan []string),
		results:     make(chan *customer),
	}
}

func (c *dataInfo) setColumns(rec []string) {
	for idx, value := range rec {
		switch {
		case regexp.MustCompile(`(?i)ful.+me`).MatchString(value):
			c.columns["fullname"] = idx
		case regexp.MustCompile(`(?i)fir.+me`).MatchString(value):
			c.columns["firstname"] = idx
		case regexp.MustCompile(`(?i)^mi$`).MatchString(value):
			c.columns["mi"] = idx
		case regexp.MustCompile(`(?i)las.+me`).MatchString(value):
			c.columns["lastname"] = idx
		case regexp.MustCompile(`(?i)^address$`).MatchString(value):
			c.columns["address1"] = idx
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
		case regexp.MustCompile(`(?i)^4zip$`).MatchString(value):
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
			name := names.Parse(record[c.columns[header]])
			customer.Firstname = tCase(name.FirstName)
			customer.MI = tCase(name.MiddleName)
			customer.Lastname = tCase(name.LastName)
		case "firstname":
			customer.Firstname = tCase(record[c.columns[header]])
		case "mi":
			customer.MI = tCase(record[c.columns[header]])
		case "lastname":
			customer.Lastname = tCase(record[c.columns[header]])
		case "address1":
			customer.Address1 = tCase(record[c.columns[header]])
		case "address2":
			customer.Address2 = tCase(record[c.columns[header]])
		case "city":
			customer.City = tCase(record[c.columns[header]])
		case "state":
			customer.State = uCase(record[c.columns[header]])
		case "zip":
			if _, ok := c.columns["zip4"]; ok {
				customer.Zip, _ = parseZip(record[c.columns[header]])
			} else {
				customer.Zip, customer.Zip4 = parseZip(record[c.columns[header]])
			}
		case "zip4":
			if _, ok := c.columns["zip4"]; ok {
				customer.Zip4 = record[c.columns[header]]
			}
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
	customer.ErrStat = c.checkforBuss(customer.Firstname)
	customer.ErrStat = c.checkforBuss(customer.MI)
	customer.ErrStat = c.checkforBuss(customer.Lastname)

	if _, ok := c.coordinates[customer.Zip]; ok {
		clat1, clon2, rlat1, rlon2 := c.getLatLong(strconv.Itoa(c.config.CentZip), customer.Zip)
		customer.Radius = fmt.Sprintf("%.2f", distance(clat1, clon2, rlat1, rlon2))
	} else {
		customer.ErrStat = "Invalid ZIP code"
	}
	return customer
}

func (c *dataInfo) getLatLong(cZip, rZip string) (float64, float64, float64, float64) {
	recCor := c.coordinates[rZip]
	cenCor := c.coordinates[cZip]
	// convert Coordinates tin FLoat64
	lat1, err := strconv.ParseFloat(cenCor[0], 64)
	lon1, err := strconv.ParseFloat(cenCor[1], 64)
	lat2, err := strconv.ParseFloat(recCor[0], 64)
	lon2, err := strconv.ParseFloat(recCor[1], 64)
	if err != nil {
		log.Fatalln("Error processing coordinates", err)
	}
	return lat1, lon1, lat2, lon2
}

func (c *dataInfo) checkforBuss(s string) string {
	names := strings.Fields(s)
	for _, name := range names {
		if _, ok := c.DNM[tCase(name)]; ok {
			return "Business Name"
		}
	}
	return ""
}

func comb(cust *customer, p *dataInfo) string {
	if _, ok := p.columns["address2"]; ok {
		return fmt.Sprintf("%v %v %v", cust.Address1, cust.Address2, cust.Zip)
	}
	return fmt.Sprintf("%v %v", cust.Address1, cust.Zip)
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
	log.Println("Ingesting Data...")
	file, err := os.Open(fmt.Sprintf("./%v.csv", fileName))
	checkErr(err)
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
	log.Println("Data Ingest Complete...")
}

func process(param *dataInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range param.tasks {
		param.results <- param.process(task)
	}
}

func addSep(n int64) string {
	in := strconv.FormatInt(n, 10)
	out := make([]byte, len(in)+(len(in)-2+int(in[0]/'0'))/3)
	if in[0] == '-' {
		in, out[0] = in[1:], '-'
	}

	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}

func hsin(theta float64) float64 {
	// haversin(θ) function
	return math.Pow(math.Sin(theta/2), 2)
}

func distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians, must cast radius as float to multiply later
	var la1, lo1, la2, lo2, rad float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180
	rad = 3959 // Earth radius in Miles
	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)
	return 2 * rad * math.Asin(math.Sqrt(h))
}

func genSeqNum() func() int {
	i := 0
	return func() int {
		i++
		return i
	}
}
