package tests

import (
	"testing"

	"github.com/aaroncutress/gtfs-go"
)

// Tests downloading and creating a GTFS database from a URL
func TestGTFSDownload(t *testing.T) {
	url := "https://www.transperth.wa.gov.au/TimetablePDFs/GoogleTransit/Production/google_transit.zip"

	// tempDir := t.TempDir()
	// dbFile := tempDir + "/test.db"
	dbFile := "test.db"

	g := &gtfs.GTFS{}
	err := g.FromURL(url, dbFile)
	if err != nil {
		t.Fatalf("Failed to create GTFS: %v", err)
		return
	}
}

// // Tests loading a GTFS database from a file
// func TestLoadNonExistentDB(t *testing.T) {
// 	dbFile := "non_existent.db"
// 	g, err := gtfs.LoadGTFSFromDB(dbFile)
// 	if err == nil {
// 		g.Close()
// 		t.Fatalf("Expected error when loading non-existent DB, got nil")
// 		return
// 	}

// 	t.Logf("Expected error when loading non-existent DB: %v", err)
// }
