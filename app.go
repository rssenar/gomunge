package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
)

func tasksFactory() {
	reader := csv.NewReader(os.Stdin)
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
	log.Println("Import process complete")
}

func processTasks() {
	defer wg.Done()
	for t := range param.tasks {
		param.results <- param.parseColumns(t)
	}
}

func outputCSV() {
	writer := csv.NewWriter(os.Stdout)
	for x := range param.results {
		var r []string
		r = append(r, x.Firstname)
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
		writer.Write(r)
	}
	writer.Flush()
}
