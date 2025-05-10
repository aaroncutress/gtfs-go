package tests

import (
	"os"
	"testing"

	"github.com/aaroncutress/gtfs-go"
	"github.com/charmbracelet/log"
)

var g *gtfs.GTFS

func TestMain(m *testing.M) {
	log.Info("Starting GTFS tests")

	// Download sample GTFS data
	g = &gtfs.GTFS{}
	err := g.FromURL(gtfsURL, dbFile)
	if err != nil {
		log.Errorf("Failed to create GTFS from URL: %v", err)
		os.Exit(1)
	}

	// Run the tests
	exitCode := m.Run()

	// Clean up the test database
	err = g.Close()
	if err != nil {
		log.Errorf("Failed to close GTFS: %v", err)
	}

	if err := os.Remove(dbFile); err != nil {
		log.Errorf("Failed to remove test database: %v", err)
	}

	// Exit with the test result
	log.Info("GTFS tests completed")
	os.Exit(exitCode)
}
