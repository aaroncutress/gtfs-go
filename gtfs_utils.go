package gtfs

import (
	"time"

	"github.com/aaroncutress/gtfs-go/models"
	"github.com/charmbracelet/log"
	"github.com/kelindar/column"
)

// Checks if the given trip is running today
func (g *GTFS) IsRunningToday(tripID models.Key) (bool, error) {
	// Retrieve the trip and service information
	trip, err := g.GetTripByID(tripID)
	if err != nil {
		return false, err
	}
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
					return true, nil
				}
			}
		}

		// If the service is not running today and there are no exceptions, return false
		return false, nil
	}

	// If the service is running today and there are exceptions, return true
	if len(exceptions) == 0 {
		return true, nil
	}

	// Check if any exceptions are set for today and whether the service is removed
	for _, exception := range exceptions {
		if exception.Date == today && exception.Type == models.RemovedExceptionType {
			return false, nil
		}
	}

	// All is well
	return true, nil
}

// Returns all trips that are currently running
func (g *GTFS) GetAllCurrentTrips() ([]*models.Trip, error) {
	// Get all trips from the database
	log.Info("Fetching all trips from the database")

	trips := make([]*models.Trip, 0)
	err := g.db.Trips.Query(func(txn *column.Txn) error {
		var err error
		trips, err = models.LoadAllTrips(txn)
		return err
	})
	if err != nil {
		return nil, err
	}

	log.Info("Fetched and decoded trips")

	now := time.Now()
	routeTzMap := make(map[models.Key]*time.Location)

	currentTrips := make([]*models.Trip, 0)

	log.Info("Checking each trip for current status")

	for _, trip := range trips {
		// Check if the trip is running today
		isRunning, err := g.IsRunningToday(trip.ID)
		if err != nil {
			return nil, err
		}
		if !isRunning {
			continue
		}

		// Get the current time in the trip's timezone
		tz, ok := routeTzMap[trip.RouteID]
		if !ok {
			agency, err := g.GetAgencyByRouteID(trip.RouteID)
			if err != nil {
				return nil, err
			}
			tz, err = time.LoadLocation(agency.Timezone)
			if err != nil {
				return nil, err
			}
			routeTzMap[trip.RouteID] = tz
		}

		nowTz := now.In(tz)
		nowTzTruncated := nowTz.Truncate(24 * time.Hour)

		// Get the trip start and end times
		tripStart := nowTzTruncated.Add(time.Duration(trip.StartTime()) * time.Second)
		if models.IsNextDay(trip.StartTime()) {
			tripStart = tripStart.Add(24 * time.Hour)
		}
		tripEnd := nowTzTruncated.Add(time.Duration(trip.EndTime()) * time.Second)
		if models.IsNextDay(trip.EndTime()) {
			tripEnd = tripEnd.Add(24 * time.Hour)
		}

		// Check if the trip is currently running
		if tripStart.Before(nowTz) && tripEnd.After(nowTz) {
			currentTrips = append(currentTrips, trip)
		}
	}

	return currentTrips, nil
}
