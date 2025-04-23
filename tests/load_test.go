package tests

// Commented for the moment because it takes too fkn long

// Tests downloading and creating a GTFS database from a URL
// func TestGTFSDownload(t *testing.T) {
// 	url := "https://www.transperth.wa.gov.au/TimetablePDFs/GoogleTransit/Production/google_transit.zip"

// 	tempDir := t.TempDir()
// 	dbFile := tempDir + "/test.db"

// 	g, err := gtfs.NewGTFSFromURL(url, dbFile)
// 	if err != nil {
// 		t.Fatalf("Failed to create GTFS from URL: %v", err)
// 		return
// 	}
// 	g.Close()
// }

// Tests loading a GTFS database from a file
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
