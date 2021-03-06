package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

var (
	configPath    = "/Users/richardsenar/Dropbox/Resource/config.json"
	zipCorPath    = "/Users/richardsenar/Dropbox/Resource/USZIPCoordinates.csv"
	scfFacPath    = "/Users/richardsenar/Dropbox/Resource/SCFFacilites.csv"
	dduFacPath    = "/Users/richardsenar/Dropbox/Resource/DDUFacilites.csv"
	ethnicityPath = "/Users/richardsenar/Dropbox/Resource/HispLNames.csv"
	dnmPath       = "/Users/richardsenar/Dropbox/Resource/DoNotMail.csv"
	gensPath      = "/Users/richardsenar/Dropbox/HUB/Projects/PyToolkit/Resources/_GeneralSuppression.csv"
)

type initConfig struct {
	Method              string `json:"Method"`
	Source              string `json:"Source"`
	CentZip             int    `json:"Central Zip"`
	MaxRadius           int    `json:"Max Radius"`
	MaxRadiusOuter      int    `json:"Max Radius Outer"`
	MaxVehYear          int    `json:"Max Vehicle Year"`
	MinVehYear          int    `json:"Min Vehicle Year"`
	MaxYearDelDate      int    `json:"Max Year DelDate"`
	MinYearDelDate      int    `json:"Min Year DelDate"`
	MaxYearDate         int    `json:"Max Year Date"`
	MinYearDate         int    `json:"Min Year Date"`
	Vendor              string `json:"Vendor"`
	ExcludeBlankDELDATE bool   `json:"Exclude Blank DelDate"`
	ExcludeBlankDATE    bool   `json:"Exclude Blank Date"`
	ExcludeBlankYear    bool   `json:"Exclude Blank Year"`
	ExcludeBlankMake    bool   `json:"Exclude Blank Make"`
	ExcludeBlankModel   bool   `json:"Exclude Blank Model"`
	ExcludeBlankVIN     bool   `json:"Exclude Blank VIN"`
	OutputPhoneList     bool   `json:"Output Phone List"`
	Gorutines           int    `json:"Gorutines"`
	IncludeZIP          []int  `json:"Include ZIP"`
}

func loadConfig() *initConfig {
	conf, err := os.Open(configPath)
	if err != nil {
		log.Fatalln("Cannot open config file", err)
	}
	defer conf.Close()

	var param *initConfig
	jsonParser := json.NewDecoder(conf)
	if err = jsonParser.Decode(&param); err != nil {
		log.Fatalln("error decoding config file", err)
	}

	switch tCase(param.Method) {
	case "Standard":
		fmt.Printf("Method:: %v\n", uCase(param.Method))
	case "Basic":
		fmt.Printf("Method:: %v\n", uCase(param.Method))
		fmt.Println("***SUPPRESSIONS DISABLED***")
	default:
		log.Fatalln("Invalid METHOD: Needs to be [Standard] or [Basic]")
	}

	switch tCase(param.Source) {
	case "Database":
		fmt.Printf("Data Source:: %v\n", uCase(param.Source))
	case "Purchase":
		fmt.Printf("Data Source:: %v\n", uCase(param.Source))
	default:
		log.Fatalln("Invalid SOURCE: Needs to be [Database] or [Purchase]")
	}
	return param
}

func loadZipCor() map[string][]string {
	zipCor, err := os.Open(zipCorPath)
	if err != nil {
		log.Fatalln("Cannot open ZipCoord file", err)
	}
	defer zipCor.Close()

	cord := make(map[string][]string)

	rdr := csv.NewReader(zipCor)
	for {
		z, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		cord[z[0]] = []string{z[1], z[2]}
	}
	return cord
}

func loadSCFFac() map[string]string {
	f, err := os.Open(scfFacPath)
	if err != nil {
		log.Fatalln("Cannot open SCFFac file", err)
	}
	defer f.Close()

	scf := make(map[string]string)

	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		scf[s[0]] = s[1]
	}
	return scf
}

func loadDDUFac() map[string]string {
	f, err := os.Open(dduFacPath)
	if err != nil {
		log.Fatalln("Cannot open DDUFac file", err)
	}
	defer f.Close()

	ddu := make(map[string]string)

	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		ddu[s[0]] = s[1]
	}
	return ddu
}

func loadEthnicity() map[string]int {
	f, err := os.Open(ethnicityPath)
	if err != nil {
		log.Fatalln("Cannot open Hisp file", err)
	}
	defer f.Close()

	hisp := make(map[string]int)

	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		hisp[tCase(s[0])]++
	}
	return hisp
}

func loadDNM() map[string]int {
	f, err := os.Open(dnmPath)
	if err != nil {
		log.Fatalln("Cannot open DNM file", err)
	}
	defer f.Close()

	dnm := make(map[string]int)

	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		dnm[tCase(s[0])]++
	}
	return dnm
}

func loadGenS() map[string]int {
	f, err := os.Open(gensPath)
	if err != nil {
		log.Fatalln("Cannot open GenSup file", err)
	}
	defer f.Close()

	gen := make(map[string]int)

	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		adrZip := fmt.Sprintf("%v %v", tCase(s[2]), tCase(s[5]))
		gen[adrZip]++
	}
	return gen
}
