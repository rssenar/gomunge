package main

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

func main() {
	fmt.Println("BEGIN...")
	timer := time.Now()
	// Initialize data pters
	p := newDataInfo()

	// Validate Central Zip code
	if _, ok := p.coordinates[strconv.Itoa(p.config.CentZip)]; !ok {
		log.Fatalln("Invalid central ZIP code")
	}
	fmt.Printf("Central ZIP code : %v\n", p.config.CentZip)

	// read CSV file from Stdin and send to the task channel
	go p.taskGenerator()

	p.wg.Add(p.config.Gorutines)
	fmt.Println("Processing...")
	for i := 0; i < p.config.Gorutines; i++ {
		go p.processTasks()
	}

	// wait for goroutines to finish
	go func() {
		p.wg.Wait()
		close(p.results)
	}()

	// range over task channel to drain channel
	// create 3 slices: output, dupes & phones
	var (
		outputRecs []*customer
		ErrRecs    []*customer
	)

	counter := 0
	for c := range p.results {
		// Tag Duplicate Address on file
		if cnt, ok := p.dupes[c.combDedupe()]; ok {
			c.ErrStat = fmt.Sprintf("Err: Duplicate Address (%v)", cnt)
		}
		p.dupes[c.combDedupe()]++

		if tCase(p.config.Method) == "Standard" {
			// Tag Duplicate Address with Gen suppression file
			if cnt, ok := p.GenSupp[c.combDedupe()]; ok {
				c.ErrStat = fmt.Sprintf("Err: Suppression File Duplicate (%v)", cnt)
			}
			// Tag Duplicate VIN numbers
			if c.VIN != "" {
				if cnt, ok := p.VIN[c.VIN]; ok {
					c.ErrStat = fmt.Sprintf("Err: Duplicate VIN (%v)", cnt)
				}
			}
			p.VIN[c.VIN]++
		}
		// Append record to corresponding array
		switch {
		case c.ErrStat != "":
			ErrRecs = append(ErrRecs, c)
		default:
			outputRecs = append(outputRecs, c)
		}
		counter++
	}

	// Generate output files if available
	if len(outputRecs) != 0 {
		fmt.Printf("Exporting Output File... %v records\n", addSep(len(outputRecs)))
		p.outputCSV(outputRecs) // Main output
		if p.config.OutputPhoneList == true {
			p.phonesCSV(outputRecs) // output available phones
		}
	}

	if len(ErrRecs) != 0 {
		fmt.Printf("Exporting Error File.... %v records\n", addSep(len(ErrRecs)))
		p.errStatusCSV(ErrRecs) // output dupes if available
	}

	fmt.Printf("ENDING... processed %v records in %v\n", addSep(counter), time.Since(timer))
}
