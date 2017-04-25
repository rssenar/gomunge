package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

type pk struct {
	Head    string `json:"head"`
	Counter int    `json:"primary_key_counter"`
}

type customer struct {
	pk
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
	DSFwalkseq string    `json:"DSF_Walk_Sequence"`
	CRRT       string    `json:"CRRT"`
	KBB        string    `json:"KBB"`
	ErrStat    string    `json:"Status"`
}

func main() {
	bkt := []byte("customer")

	// open boltDB database
	db := initializeDB()
	defer db.Close()

	log.Println("Processing Data...")
	// set timer & primKeySeq
	timer := time.Now()
	pkCounter := genPrimaryKeyCounter()

	// Initialize data paramters
	param := newDataInfo()

	var wg sync.WaitGroup

	// set wait group to terminate after worker
	// go routines have all finished
	go func() {
		wg.Wait()
		close(param.results)
	}()
	// read CSV file from Stdin and send to the task channel
	go taskGenerator(param)
	wg.Add(gophers)
	for i := 0; i < gophers; i++ {
		go process(param, &wg)
	}

	// Create CSV output file
	sendOut, fout := output()
	defer fout.Close()
	// Create Dupes output file
	sendErrStat, fdupe := errStatus()
	defer fdupe.Close()
	// Create Phones output file
	sendPhone, fphone := outputPhones()
	defer fphone.Close()

	// Range over reuslts channel and check for ducplicate records
	// output clean CSV, Duplicates & Phone files
	for c := range param.results {
		c.pk.Head = fileName
		c.pk.Counter = pkCounter()

		// Check for Duplicate Address
		if cnt, ok := param.dupes[comb(c)]; ok {
			c.ErrStat = fmt.Sprintf("Duplicate Address (%v)", cnt)
		}
		param.dupes[comb(c)]++

		// Check for Duplicate VIN numbers
		if c.VIN != "" {
			if cnt, ok := param.VIN[c.VIN]; ok {
				c.ErrStat = fmt.Sprintf("Duplicate VIN (%v)", cnt)
			}
		}
		param.VIN[c.VIN]++

		// output processed and duplicate files
		switch {
		case c.ErrStat == "":
			sendOut(c)
			sendPhone(c)
			// add to database
			db.Update(func(tx *bolt.Tx) error {
				keyBytes, err := marshal(c.pk)
				if err != nil {
					return err
				}
				dataBytes, err := marshal(c)
				if err != nil {
					return err
				}
				b, err := tx.CreateBucketIfNotExists(bkt)
				if err != nil {
					return err
				}
				err = b.Put(keyBytes, dataBytes)
				if err != nil {
					return err
				}
				return nil
			})

		default:
			sendErrStat(c)
		}
	}

	log.Printf("Completed in %v\n", time.Since(timer))
}
