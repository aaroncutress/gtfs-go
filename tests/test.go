package main

import (
	"github.com/aaroncutress/gtfs-go"
	"github.com/charmbracelet/log"
)

func main() {
	// g, err := gtfs.NewGTFSFromURL("https://www.transperth.wa.gov.au/TimetablePDFs/GoogleTransit/Production/google_transit.zip", "transperth.db")
	g, err := gtfs.LoadGTFSFromDB("transperth.db")
	if err != nil {
		log.Errorf("Error creating GTFS database: %v", err)
		return
	}
	defer g.Close()
}
