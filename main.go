package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

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
	var ErrRecs []*customer
	var phonesRecs []*customer

	counter := 0
	for c := range param.results {
		// Check for Duplicate Address on file
		if cnt, ok := param.dupes[comb(c, param)]; ok {
			c.ErrStat = fmt.Sprintf("Duplicate Address (%v)", cnt)
		}
		param.dupes[comb(c, param)]++

		// Check for Duplicate Address with Gen suppression file
		if cnt, ok := param.GenSupp[comb(c, param)]; ok {
			c.ErrStat = fmt.Sprintf("Suppression File Duplicate (%v)", cnt)
		}

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
			ErrRecs = append(ErrRecs, c)
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
	if len(ErrRecs) != 0 {
		errStatusCSV(ErrRecs) // output dupes if available
	}
	log.Printf("Completed! processed %v records in %v\n", counter, time.Since(timer))
}
