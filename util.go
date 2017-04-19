package main

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blendlabs/go-name-parser"
)

type columnInfo struct {
	columns map[string]int
}

func newColumnInfo() *columnInfo {
	return &columnInfo{columns: make(map[string]int)}
}

func (c *columnInfo) setColumns(record []string) {
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

func (c *columnInfo) parseColumns(record []string, rowNum int) *customer {
	customer := &customer{id: rowNum}
	for header := range c.columns {
		switch header {
		case "fullname":
			name := names.Parse(record[c.columns[header]])
			if _, ok := c.columns["firstname"]; ok {
				if record[c.columns["firstname"]] == "" {
					customer.firstname = tCase(name.FirstName)
				}
			}
			if _, ok := c.columns["mi"]; ok {
				if record[c.columns["mi"]] == "" {
					customer.mi = tCase(name.MiddleName)
				}
			}
			if _, ok := c.columns["lastname"]; ok {
				if record[c.columns["lastname"]] == "" {
					customer.lastname = tCase(name.LastName)
				}
			}
		case "firstname":
			if customer.firstname == "" {
				customer.firstname = tCase(record[c.columns[header]])
			}
		case "mi":
			if customer.mi == "" {
				customer.mi = tCase(record[c.columns[header]])
			}
		case "lastname":
			if customer.lastname == "" {
				customer.lastname = tCase(record[c.columns[header]])
			}
		case "address1":
			customer.address1 = tCase(record[c.columns[header]])
		case "address2":
			customer.address2 = tCase(record[c.columns[header]])
		case "city":
			customer.city = tCase(record[c.columns[header]])
		case "state":
			customer.state = tCase(record[c.columns[header]])
		case "zip":
			customer.zip, _ = parseZip(record[c.columns[header]])
		case "zip4":
			_, customer.zip4 = parseZip(record[c.columns[header]])
		case "hph":
			customer.hph = parsePhone(record[c.columns[header]])
		case "bph":
			customer.bph = parsePhone(record[c.columns[header]])
		case "cph":
			customer.cph = parsePhone(record[c.columns[header]])
		case "email":
			customer.email = lCase(record[c.columns[header]])
		case "vin":
			customer.vin = uCase(record[c.columns[header]])
		case "year":
			if _, err := strconv.Atoi(record[c.columns[header]]); err != nil {
				log.Printf("Invalid Year: %v %v on record (%v)", header, err, customer.id)
				customer.year = ""
			} else {
				customer.year = decodeYr(record[c.columns[header]])
			}
		case "make":
			customer.make = tCase(record[c.columns[header]])
		case "model":
			customer.model = tCase(record[c.columns[header]])
		case "deldate":
			customer.deldate = parseDate(record[c.columns[header]])
		case "date":
			customer.date = parseDate(record[c.columns[header]])
		case "dsfwalkseq":
			if _, err := strconv.Atoi(record[c.columns[header]]); err != nil {
				log.Printf("%v Invalid WalkSeq on record (%v)", err, customer.id)
				customer.dsfwalkseq = ""
			} else {
				customer.dsfwalkseq = record[c.columns[header]]
			}
		case "crrt":
			customer.crrt = uCase(record[c.columns[header]])
		case "kbb":
			if _, err := strconv.Atoi(record[c.columns[header]]); err != nil {
				log.Printf("%v could not parse KBB on record (%v)", err, customer.id)
				customer.kbb = ""
			} else {
				customer.kbb = record[c.columns[header]]
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
	sep := []string{"-", ".", "*", "(", ")"}
	for _, v := range sep {
		p = strings.Replace(p, v, "", -1)
	}
	p = strings.Replace(p, " ", "", -1)
	switch len(p) {
	case 10:
		return fmt.Sprintf("(%v) %v-%v", p[0:3], p[3:6], p[6:10])
	case 7:
		return fmt.Sprintf("%v-%v", p[0:3], p[3:7])
	default:
		return ""
	}
}

func decodeYr(y string) string {
	// YearDecodeDict is a map of 2-Digit abbreviated Years
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
