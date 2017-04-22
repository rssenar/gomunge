package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blendlabs/go-name-parser"
)

type dataInfo struct {
	columns     map[string]int
	dupes       map[string]int
	suppression map[string]string
	tasks       chan []string
	results     chan *customer
	duplicates  chan *customer
}

func newDataInfo() *dataInfo {
	return &dataInfo{
		columns:     make(map[string]int),
		dupes:       make(map[string]int),
		suppression: make(map[string]string),
		tasks:       make(chan []string),
		results:     make(chan *customer),
		duplicates:  make(chan *customer),
	}
}

func (c *dataInfo) deDupe(cust *customer) (*customer, error) {
	addr := fmt.Sprintf("%v %v %v", cust.Address1, cust.Address2, cust.Zip)
	if _, ok := c.dupes[addr]; ok {
		err := fmt.Errorf("%v is a duplicate record", addr)
		return nil, err
	}
	c.dupes[addr]++
	return cust, nil
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

func (c *dataInfo) parseColumns(record []string) *customer {
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

func outputCSV() {
	writer := csv.NewWriter(os.Stdout)
	header := []string{
		"First Name",
		"MI",
		"Last Name",
		"Address",
		"City",
		"State",
		"Zip",
		"Home Phone",
		"Business Phone",
		"Mobile Phone",
		"VIN #",
		"Year",
		"Make",
		"Model",
		"Delivery Date",
		"Last Serv Date",
		"Walk Seq",
		"CRRT",
		"KBB",
		"DelDate_Year",
		"DelDate_Month",
		"Date_Year",
		"Date_Month",
	}
	writer.Write(header)
	for x := range param.results {
		var r []string
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
		writer.Write(r)
	}
	writer.Flush()
}

func decodeYr(y string) string {
	yrDecDict := map[string]string{
		"0":  "2000",
		"1":  "2001",
		"2":  "2002",
		"3":  "2003",
		"4":  "2004",
		"5":  "2005",
		"6":  "2006",
		"7":  "2007",
		"8":  "2008",
		"9":  "2009",
		"10": "2010",
		"11": "2011",
		"12": "2012",
		"13": "2013",
		"14": "2014",
		"15": "2015",
		"16": "2016",
		"17": "2017",
		"18": "2018",
		"19": "2019",
		"20": "2020",
		"40": "1940",
		"41": "1941",
		"42": "1942",
		"43": "1943",
		"44": "1944",
		"45": "1945",
		"46": "1946",
		"47": "1947",
		"48": "1948",
		"49": "1949",
		"50": "1950",
		"51": "1951",
		"52": "1952",
		"53": "1953",
		"54": "1954",
		"55": "1955",
		"56": "1956",
		"57": "1957",
		"58": "1958",
		"59": "1959",
		"60": "1960",
		"61": "1961",
		"62": "1962",
		"63": "1963",
		"64": "1964",
		"65": "1965",
		"66": "1966",
		"67": "1967",
		"68": "1968",
		"69": "1969",
		"70": "1970",
		"71": "1971",
		"72": "1972",
		"73": "1973",
		"74": "1974",
		"75": "1975",
		"76": "1976",
		"77": "1977",
		"78": "1978",
		"79": "1979",
		"80": "1980",
		"81": "1981",
		"82": "1982",
		"83": "1983",
		"84": "1984",
		"85": "1985",
		"86": "1986",
		"87": "1987",
		"88": "1988",
		"89": "1989",
		"90": "1990",
		"91": "1991",
		"92": "1992",
		"93": "1993",
		"94": "1994",
		"95": "1995",
		"96": "1996",
		"97": "1997",
		"98": "1998",
		"99": "1999",
	}
	if dy, ok := yrDecDict[y]; ok {
		return dy
	}
	return y
}

func decAbSt(s string) string {
	usStDict := map[string]string{
		"AK": "Alaska",
		"AL": "Alabama",
		"AR": "Arkansas",
		"AS": "American Samoa",
		"AZ": "Arizona",
		"CA": "California",
		"CO": "Colorado",
		"CT": "Connecticut",
		"DC": "District of Columbia",
		"DE": "Delaware",
		"FL": "Florida",
		"GA": "Georgia",
		"GU": "Guam",
		"HI": "Hawaii",
		"IA": "Iowa",
		"ID": "Idaho",
		"IL": "Illinois",
		"IN": "Indiana",
		"KS": "Kansas",
		"KY": "Kentucky",
		"LA": "Louisiana",
		"MA": "Massachusetts",
		"MD": "Maryland",
		"ME": "Maine",
		"MI": "Michigan",
		"MN": "Minnesota",
		"MO": "Missouri",
		"MP": "Northern Mariana Islands",
		"MS": "Mississippi",
		"MT": "Montana",
		"NA": "National",
		"NC": "North Carolina",
		"ND": "North Dakota",
		"NE": "Nebraska",
		"NH": "New Hampshire",
		"NJ": "New Jersey",
		"NM": "New Mexico",
		"NV": "Nevada",
		"NY": "New York",
		"OH": "Ohio",
		"OK": "Oklahoma",
		"OR": "Oregon",
		"PA": "Pennsylvania",
		"PR": "Puerto Rico",
		"RI": "Rhode Island",
		"SC": "South Carolina",
		"SD": "South Dakota",
		"TN": "Tennessee",
		"TX": "Texas",
		"UT": "Utah",
		"VA": "Virginia",
		"VI": "Virgin Islands",
		"VT": "Vermont",
		"WA": "Washington",
		"WI": "Wisconsin",
		"WV": "West Virginia",
		"WY": "Wyoming"}
	if ds, ok := usStDict[s]; ok {
		return ds
	}
	return s
}
