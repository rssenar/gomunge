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
	ID          int       `json:"id"`
	Fullname    string    `json:"full_name"`
	Firstname   string    `json:"first_name"`
	MI          string    `json:"middle_name"`
	Lastname    string    `json:"last_name"`
	Address1    string    `json:"address_1"`
	Address2    string    `json:"address_2"`
	City        string    `json:"city"`
	State       string    `json:"state"`
	Zip         string    `json:"zip"`
	Zip4        string    `json:"zip_4"`
	HPH         string    `json:"home_phone"`
	BPH         string    `json:"business_phone"`
	CPH         string    `json:"mobile_phone"`
	Email       string    `json:"email"`
	VIN         string    `json:"VIN"`
	Year        string    `json:"year"`
	Make        string    `json:"make"`
	Model       string    `json:"model"`
	DelDate     time.Time `json:"delivery_date"`
	Date        time.Time `json:"date"`
	Radius      string    `json:"radius"`
	Coordinates string    `json:"coordinates"`
	DSFwalkseq  string    `json:"DSF_Walk_Sequence"`
	CRRT        string    `json:"CRRT"`
	KBB         string    `json:"KBB"`
	ErrStat     string    `json:"Status"`
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
	wg          sync.WaitGroup
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

func (d *dataInfo) setColumns(rec []string) {
	for idx, value := range rec {
		switch {
		case regexp.MustCompile(`(?i)ful.+me`).MatchString(value):
			d.columns["fullname"] = idx
		case regexp.MustCompile(`(?i)fir.+me`).MatchString(value):
			d.columns["firstname"] = idx
		case regexp.MustCompile(`(?i)^mi$`).MatchString(value):
			d.columns["mi"] = idx
		case regexp.MustCompile(`(?i)las.+me`).MatchString(value):
			d.columns["lastname"] = idx
		case regexp.MustCompile(`(?i)^address$`).MatchString(value):
			d.columns["address1"] = idx
		case regexp.MustCompile(`(?i)addr.+1`).MatchString(value):
			d.columns["address1"] = idx
		case regexp.MustCompile(`(?i)addr.+2`).MatchString(value):
			d.columns["address2"] = idx
		case regexp.MustCompile(`(?i)^city$`).MatchString(value):
			d.columns["city"] = idx
		case regexp.MustCompile(`(?i)^state$`).MatchString(value):
			d.columns["state"] = idx
		case regexp.MustCompile(`(?i)^zip$`).MatchString(value):
			d.columns["zip"] = idx
		case regexp.MustCompile(`(?i)^zip4$`).MatchString(value):
			d.columns["zip4"] = idx
		case regexp.MustCompile(`(?i)^4zip$`).MatchString(value):
			d.columns["zip4"] = idx
		case regexp.MustCompile(`(?i)^hph$`).MatchString(value):
			d.columns["hph"] = idx
		case regexp.MustCompile(`(?i)^bph$`).MatchString(value):
			d.columns["bph"] = idx
		case regexp.MustCompile(`(?i)^cph$`).MatchString(value):
			d.columns["cph"] = idx
		case regexp.MustCompile(`(?i)^email$`).MatchString(value):
			d.columns["email"] = idx
		case regexp.MustCompile(`(?i)^vin$`).MatchString(value):
			d.columns["vin"] = idx
		case regexp.MustCompile(`(?i)^year$`).MatchString(value):
			d.columns["year"] = idx
		case regexp.MustCompile(`(?i)^vyr$`).MatchString(value):
			d.columns["year"] = idx
		case regexp.MustCompile(`(?i)^make$`).MatchString(value):
			d.columns["make"] = idx
		case regexp.MustCompile(`(?i)^vmk$`).MatchString(value):
			d.columns["make"] = idx
		case regexp.MustCompile(`(?i)^model$`).MatchString(value):
			d.columns["model"] = idx
		case regexp.MustCompile(`(?i)^vmd$`).MatchString(value):
			d.columns["model"] = idx
		case regexp.MustCompile(`(?i)^deldate$`).MatchString(value):
			d.columns["deldate"] = idx
		case regexp.MustCompile(`(?i)^date$`).MatchString(value):
			d.columns["date"] = idx
		case regexp.MustCompile(`(?i)^DSF_WALK_SEQ$`).MatchString(value):
			d.columns["dsfwalkseq"] = idx
		case regexp.MustCompile(`(?i)^Crrt$`).MatchString(value):
			d.columns["crrt"] = idx
		case regexp.MustCompile(`(?i)^KBB$`).MatchString(value):
			d.columns["kbb"] = idx
		}
	}
}

func (d *dataInfo) processRecord(record []string) *customer {
	customer := &customer{}
	for header := range d.columns {
		switch header {
		case "fullname":
			customer.Fullname = tCase(record[d.columns[header]])
		case "firstname":
			customer.Firstname = tCase(record[d.columns[header]])
		case "mi":
			customer.MI = tCase(record[d.columns[header]])
		case "lastname":
			customer.Lastname = tCase(record[d.columns[header]])
		case "address1":
			customer.Address1 = tCase(record[d.columns[header]])
		case "address2":
			customer.Address2 = tCase(record[d.columns[header]])
		case "city":
			customer.City = tCase(record[d.columns[header]])
		case "state":
			customer.State = uCase(record[d.columns[header]])
		case "zip":
			customer.Zip = record[d.columns[header]]
		case "zip4":
			customer.Zip4 = record[d.columns[header]]
		case "hph":
			customer.HPH = formatPhone(record[d.columns[header]])
		case "bph":
			customer.BPH = formatPhone(record[d.columns[header]])
		case "cph":
			customer.CPH = formatPhone(record[d.columns[header]])
		case "email":
			customer.Email = lCase(record[d.columns[header]])
		case "vin":
			customer.VIN = uCase(record[d.columns[header]])
		case "year":
			customer.Year = decodeYr(trimZeros(stripSep(record[d.columns[header]])))
		case "make":
			customer.Make = tCase(record[d.columns[header]])
		case "model":
			customer.Model = tCase(record[d.columns[header]])
		case "deldate":
			customer.DelDate = parseDate(record[d.columns[header]])
		case "date":
			customer.Date = parseDate(record[d.columns[header]])
		case "dsfwalkseq":
			customer.DSFwalkseq = stripSep(record[d.columns[header]])
		case "crrt":
			customer.CRRT = uCase(record[d.columns[header]])
		case "kbb":
			customer.KBB = stripSep(record[d.columns[header]])
		}
	}

	if customer.Firstname == "" && customer.Lastname == "" {
		name := names.Parse(customer.Fullname)
		customer.Firstname = tCase(name.FirstName)
		customer.MI = tCase(name.MiddleName)
		customer.Lastname = tCase(name.LastName)
	}

	if customer.Firstname == "" || customer.Lastname == "" {
		customer.ErrStat = "Err: Missing First/Last Name"
	}

	zip, zip4 := parseZip(customer.Zip)
	customer.Zip = zip
	if zip4 != "" {
		customer.Zip4 = zip4
	}

	if _, ok := d.coordinates[customer.Zip]; ok {
		customer.Coordinates = strings.Join(d.coordinates[customer.Zip], ", ")
		clat1, clon2, rlat1, rlon2 := d.getLatLong(strconv.Itoa(d.config.CentZip), customer.Zip)
		customer.Radius = fmt.Sprintf("%.2f", distance(clat1, clon2, rlat1, rlon2))
	} else {
		customer.ErrStat = "Err: Invalid ZIP Code"
	}

	if tCase(d.config.Method) == "Standard" {
		if rad, err := strconv.ParseFloat(customer.Radius, 64); err == nil {
			if rad > float64(d.config.MaxRadius) {
				customer.ErrStat = "Err: Max Radius Exceeded"
			}
		}
		for _, zinc := range d.config.IncludeZIP {
			if customer.Zip == trimZeros(strconv.Itoa(zinc)) {
				customer.ErrStat = ""
			}
		}
		if yr, err := strconv.Atoi(customer.Year); err == nil {
			if yr > d.config.MaxVehYear {
				customer.ErrStat = "Err: Max Year Exceeded"
			}
		}
		if yr, err := strconv.Atoi(customer.Year); err == nil {
			if yr < d.config.MinVehYear {
				customer.ErrStat = "Err: Min Year Exceeded"
			}
		}
		if d.config.ExcludeBlankYear {
			if customer.Year == "" {
				customer.ErrStat = "Err: Blank Year"
			}
		}
		if d.config.ExcludeBlankMake {
			if customer.Make == "" {
				customer.ErrStat = "Err: Blank Make"
			}
		}
		if d.config.ExcludeBlankModel {
			if customer.Model == "" {
				customer.ErrStat = "Err: Blank Model"
			}
		}
		if d.config.ExcludeBlankVIN {
			if customer.VIN == "" {
				customer.ErrStat = "Err: Blank VIN"
			}
		}
		if d.config.ExcludeBlankDELDATE {
			if customer.DelDate.IsZero() {
				customer.ErrStat = "Err: Blank DELDATE"
			}
		}
		if d.config.ExcludeBlankDATE {
			if customer.Date.IsZero() {
				customer.ErrStat = "Err: Blank DATE"
			}
		}
		if !customer.DelDate.IsZero() {
			if customer.DelDate.Year() > d.config.MaxYearDelDate {
				customer.ErrStat = "Err: Max DelDate Exceeded"
			}
		}
		if !customer.DelDate.IsZero() {
			if customer.DelDate.Year() < d.config.MinYearDelDate {
				customer.ErrStat = "Err: Min DelDate Exceeded"
			}
		}
		if !customer.Date.IsZero() {
			if customer.Date.Year() > d.config.MaxYearDate {
				customer.ErrStat = "Err: Max Date Exceeded"
			}
		}
		if !customer.Date.IsZero() {
			if customer.Date.Year() < d.config.MinYearDate {
				customer.ErrStat = "Err: Min Date Exceeded"
			}
		}
	}

	customer.ErrStat = d.checkforBuss(customer.Firstname)
	customer.ErrStat = d.checkforBuss(customer.MI)
	customer.ErrStat = d.checkforBuss(customer.Lastname)
	return customer
}

func (d *dataInfo) getLatLong(cZip, rZip string) (float64, float64, float64, float64) {
	recCor := d.coordinates[rZip]
	cenCor := d.coordinates[cZip]
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

func (d *dataInfo) checkforBuss(s string) string {
	names := strings.Fields(s)
	for _, name := range names {
		if _, ok := d.DNM[tCase(name)]; ok {
			return "Err: Business"
		}
	}
	return ""
}

func (d *dataInfo) taskGenerator() {
	fmt.Println("Ingesting Data")
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
			d.setColumns(rec)
		} else {
			d.tasks <- rec
		}
	}
	close(d.tasks)
}

func (d *dataInfo) processTasks() {
	defer d.wg.Done()
	for task := range d.tasks {
		d.results <- d.processRecord(task)
	}
}

func (c *customer) combDedupe() string {
	if c.Address2 == "" {
		return fmt.Sprintf("%v %v", c.Address1, c.Zip)
	}
	return fmt.Sprintf("%v %v %v", c.Address1, c.Address2, c.Zip)
}

func (c *customer) combAddr(cust *customer) string {
	if c.Address2 == "" {
		return fmt.Sprintf("%v", c.Address1)
	}
	return fmt.Sprintf("%v %v", c.Address1, c.Address2)
}

func parseZip(zip string) (string, string) {
	switch {
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$`).MatchString(zip):
		return trimZeros(zip[:5]), trimZeros(zip[5:])
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]$`).MatchString(zip):
		zsplit := strings.Split(zip, "-")
		return trimZeros(zsplit[0]), trimZeros(zsplit[1])
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9][0-9] [0-9][0-9][0-9][0-9]$`).MatchString(zip):
		zsplit := strings.Split(zip, " ")
		return trimZeros(zsplit[0]), trimZeros(zsplit[1])
	default:
		return zip, ""
	}
}

func trimZeros(s string) string {
	for i := 0; i < len(s); i++ {
		s = strings.TrimPrefix(s, "0")
	}
	return s
}

func parseDate(d string) time.Time {
	if d != "" {
		formats := []string{"1/2/2006", "1-2-2006", "1/2/06", "1-2-06",
			"2006/1/2", "2006-1-2", time.RFC3339}
		for _, f := range formats {
			if date, err := time.Parse(f, d); err == nil {
				return date
			}
		}
	}
	return time.Time{}
}

func stripSep(p string) string {
	sep := []string{"'", "#", "%", "$", "-", ".", "*", "(", ")", ":", ";", "{", "}", "|", " "}
	for _, v := range sep {
		p = strings.Replace(p, v, "", -1)
	}
	return p
}

func formatPhone(p string) string {
	p = stripSep(p)
	switch len(p) {
	case 10:
		return fmt.Sprintf("(%v) %v-%v", p[0:3], p[3:6], p[6:10])
	case 7:
		return fmt.Sprintf("%v-%v", p[0:3], p[3:7])
	default:
		return ""
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

func tCase(f string) string {
	return strings.TrimSpace(strings.Title(strings.ToLower(f)))
}

func uCase(f string) string {
	return strings.TrimSpace(strings.ToUpper(f))
}

func lCase(f string) string {
	return strings.TrimSpace(strings.ToLower(f))
}

func addSep(n int) string {
	in := strconv.Itoa(n)
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

// func genSeqNum() func() int {
// 	i := 0
// 	return func() int {
// 		i++
// 		return i
// 	}
// }
