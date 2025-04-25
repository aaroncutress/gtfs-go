package gtfs

import (
	"time"

	"github.com/aaroncutress/gtfs-go/models"
	"github.com/charmbracelet/log"
)

// Checks if the given trip is running today
func isRunningToday(g *GTFS, trip *models.Trip, cache *map[models.Key]bool) (bool, error) {
	cached, ok := (*cache)[trip.ServiceID]
	if ok {
		return cached, nil
	}

	// Retrieve the trip and service information
	service, err := g.GetServiceByID(trip.ServiceID)
	if err != nil {
		return false, err
	}
	exceptions, err := g.GetServiceExceptionsByServiceID(trip.ServiceID)
	if err != nil {
		return false, err
	}

	today := time.Now().Truncate(24 * time.Hour)
	dayOfWeek := today.Weekday()

	// Check if the service is not (normally) running today
	if (service.Weekdays & (1 << dayOfWeek)) == 0 {
		if len(exceptions) > 0 {
			// Check if there are any exceptions for today
			for _, exception := range exceptions {
				if exception.Date == today && exception.Type == models.AddedExceptionType {
					(*cache)[trip.ServiceID] = true
					return true, nil
				}
			}
		}

		// If the service is not running today and there are no exceptions, return false
		(*cache)[trip.ServiceID] = false
		return false, nil
	}

	// If the service is running today and there are exceptions, return true
	if len(exceptions) == 0 {
		(*cache)[trip.ServiceID] = true
		return true, nil
	}

	// Check if any exceptions are set for today and whether the service is removed
	for _, exception := range exceptions {
		if exception.Date == today && exception.Type == models.RemovedExceptionType {
			(*cache)[trip.ServiceID] = false
			return false, nil
		}
	}

	// All is well
	(*cache)[trip.ServiceID] = true
	return true, nil
}

// Returns all trips that are currently running
func (g *GTFS) GetAllCurrentTrips() (models.TripArray, error) {
	// Get all trips from the database
	log.Info("Fetching all trips from the database")

	var trips models.TripArray
	err := g.db.Trips.Query(trips.Load)
	if err != nil {
		return nil, err
	}

	log.Infof("Fetched and decoded %d trips", len(trips))

	now := time.Now().UTC()
	nowTruncated := now.Truncate(24 * time.Hour)

	currentTrips := make(models.TripArray, len(trips))
	total := 0

	log.Info("Checking each trip for current status")

	cache := make(map[models.Key]bool)
	for _, trip := range trips {
		// Get the trip start and end times
		tripStart := nowTruncated.Add(time.Duration(trip.StartTime()) * time.Second)
		if models.IsNextDay(trip.StartTime()) {
			tripStart = tripStart.Add(24 * time.Hour)
		}
		tripEnd := nowTruncated.Add(time.Duration(trip.EndTime()) * time.Second)
		if models.IsNextDay(trip.EndTime()) {
			tripEnd = tripEnd.Add(24 * time.Hour)
		}

		// Check if the trip is currently running
		if tripStart.After(now) || tripEnd.Before(now) {
			continue
		}

		log.Debugf("Trip %s: Start %s, End %s", trip.ID, tripStart.Format(time.RFC3339), tripEnd.Format(time.RFC3339))

		// Check if the trip is running today
		isRunning, err := isRunningToday(g, trip, &cache)
		if err != nil {
			return nil, err
		}
		if !isRunning {
			continue
		}

		// Add the trip to the current trips list
		currentTrips[total] = trip
		total++
	}

	currentTrips = currentTrips[:total]

	return currentTrips, nil
}
