package gtfs

import (
	"time"

	"github.com/aaroncutress/gtfs-go/models"
	"github.com/charmbracelet/log"
)

// Check if a given weekday is present in the flags
func hasDay(flags models.WeekdayFlag, day time.Weekday) bool {
	if day < time.Sunday || day > time.Saturday {
		return false
	}

	bitPos := (int(day) - 1 + 7) % 7
	dayFlag := models.WeekdayFlag(1 << bitPos)
	return (flags & dayFlag) != 0
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
	weekday := nowTruncated.Weekday()

	currentTrips := make(models.TripArray, len(trips))
	total := 0

	log.Info("Checking each trip for current status")

	runningCache := make(map[models.Key]bool) // service id -> running
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

		// Check if the trip is running today
		running, ok := runningCache[trip.ServiceID]
		if !ok {
			service, err := g.GetServiceByID(trip.ServiceID)
			if err != nil {
				return nil, err
			}
			exception, _ := g.GetServiceException(trip.ServiceID, nowTruncated)

			if hasDay(service.Weekdays, weekday) {
				running = exception == nil || exception.Type != models.RemovedExceptionType
			} else {
				running = exception != nil && exception.Type == models.AddedExceptionType
			}

			runningCache[trip.ServiceID] = running
		}

		// Skip the trip if it's not running today
		if !running {
			continue
		}

		// Add the trip to the current trips list
		currentTrips[total] = trip
		total++
	}

	currentTrips = currentTrips[:total]
	return currentTrips, nil
}
