package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

var writer *csv.Writer

func main() {
	log.Println("Begin...")
	timer := time.Now()
	// Initialize data paramters
	param := newDataInfo()

	// Validate Central Zip code
	if _, ok := param.coordinates[strconv.Itoa(param.config.CentZip)]; !ok {
		log.Fatalln("Invalid central ZIP code")
	}

	var wg sync.WaitGroup

	// read CSV file from Stdin and send to the task channel
	go taskGenerator(param)

	wg.Add(gophers)
	log.Printf("Generating %v Goroutines...\n", gophers)
	for i := 0; i < gophers; i++ {
		go process(param, &wg)
	}

	// wait for goroutines to finish
	go func() {
		wg.Wait()
		close(param.results)
		log.Println("Goroutines Terminated...")
	}()

	// range over task channel to drain channel
	// create 3 slices: output, dupes & phones
	var outputRecs []*customer
	var duplicatesRecs []*customer
	var phonesRecs []*customer

	counter := 0
	for c := range param.results {
		// Check for Duplicate Address & update ErrStat struct info
		if cnt, ok := param.dupes[comb(c)]; ok {
			c.ErrStat = fmt.Sprintf("Duplicate Address (%v)", cnt)
		}
		param.dupes[comb(c)]++

		// Check for Duplicate VIN numbers & update ErrStat struct info
		if c.VIN != "" {
			if cnt, ok := param.VIN[c.VIN]; ok {
				c.ErrStat = fmt.Sprintf("Duplicate VIN (%v)", cnt)
			}
		}
		param.VIN[c.VIN]++

		// Append record to corresponding array
		switch {
		case c.ErrStat == "":
			outputRecs = append(outputRecs, c)
			phonesRecs = append(phonesRecs, c)
		default:
			duplicatesRecs = append(duplicatesRecs, c)
		}
		counter++
	}

	// Generate output files if available
	if len(outputRecs) != 0 {
		outputCSV(outputRecs) // Main output
	}
	if len(phonesRecs) != 0 {
		phonesCSV(phonesRecs) // output available phones
	}
	if len(duplicatesRecs) != 0 {
		errStatusCSV(duplicatesRecs) // output dupes if available
	}
	log.Printf("Completed! processed %v records in %v\n", counter, time.Since(timer))
}
