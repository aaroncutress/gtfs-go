package tests

import (
	"testing"

	"github.com/aaroncutress/gtfs-go"
	"github.com/aaroncutress/gtfs-go/models"
)

func TestGetIsTripRunningToday(t *testing.T) {
	// Create a GTFS instance
	g := &gtfs.GTFS{}
	err := g.FromDB("test.db")
	if err != nil {
		t.Fatalf("Failed to load GTFS database: %v", err)
	}

	// Get a trip ID (replace with a valid trip ID from your GTFS data)
	tripID := "2103211"

	// Check if the trip is running today
	isRunning, err := g.IsRunningToday(models.Key(tripID))
	if err != nil {
		t.Fatalf("Failed to check if trip is running today: %v", err)
	}

	if !isRunning {
		t.Logf("Trip %s is not running today", tripID)
	} else {
		t.Logf("Trip %s is running today", tripID)
	}
}

// Tests getting all current trips from the GTFS database
func TestGetCurrentTrips(t *testing.T) {
	// Create a GTFS instance
	g := &gtfs.GTFS{}
	err := g.FromDB("test.db")
	if err != nil {
		t.Fatalf("Failed to load GTFS database: %v", err)
	}

	// Get all current trips
	trips, err := g.GetAllCurrentTrips()
	if err != nil {
		t.Fatalf("Failed to get current trips: %v", err)
	}

	// Check if the number of trips is greater than 0
	if len(trips) == 0 {
		t.Fatal("No current trips found")
	}

	t.Logf("Number of current trips: %d", len(trips))
}
