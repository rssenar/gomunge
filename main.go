package main

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

func main() {
	log.Println("Begin...")
	timer := time.Now()
	// Initialize data pters
	p := newDataInfo()

	// Validate Central Zip code
	if _, ok := p.coordinates[strconv.Itoa(p.config.CentZip)]; !ok {
		log.Fatalln("Invalid central ZIP code")
	}

	// var wg sync.WaitGroup

	// read CSV file from Stdin and send to the task channel
	go p.taskGenerator()

	p.wg.Add(gophers)
	log.Printf("Generating %v Goroutines...\n", gophers)
	for i := 0; i < gophers; i++ {
		go p.processTasks()
	}

	// wait for goroutines to finish
	go func() {
		p.wg.Wait()
		close(p.results)
		log.Println("Goroutines Terminated...")
	}()

	// range over task channel to drain channel
	// create 3 slices: output, dupes & phones
	var outputRecs []*customer
	var ErrRecs []*customer
	var phonesRecs []*customer

	counter := 0
	for c := range p.results {
		// Check for Duplicate Address on file
		if cnt, ok := p.dupes[c.combDedupe()]; ok {
			c.ErrStat = fmt.Sprintf("Duplicate Address (%v)", cnt)
		}
		p.dupes[c.combDedupe()]++

		// Check for Duplicate Address with Gen suppression file
		if cnt, ok := p.GenSupp[c.combDedupe()]; ok {
			c.ErrStat = fmt.Sprintf("Suppression File Duplicate (%v)", cnt)
		}

		// Check for Duplicate VIN numbers & update ErrStat struct info
		if c.VIN != "" {
			if cnt, ok := p.VIN[c.VIN]; ok {
				c.ErrStat = fmt.Sprintf("Duplicate VIN (%v)", cnt)
			}
		}
		p.VIN[c.VIN]++

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
