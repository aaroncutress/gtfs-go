package gtfs

import (
	"time"

	"github.com/aaroncutress/gtfs-go/models"
	"github.com/charmbracelet/log"
)

const secondsInDay = 24 * 60 * 60

// Check if a given weekday is present in the flags
func hasDay(flags models.WeekdayFlag, day time.Weekday) bool {
	if day < time.Sunday || day > time.Saturday {
		return false
	}

	bitPos := (int(day) + 6) % 7
	dayFlag := models.WeekdayFlag(1 << bitPos)
	return (flags & dayFlag) != 0
}

// Returns all trips that are currently running at the given time
func (g *GTFS) GetCurrentTripsAt(t time.Time) (models.TripArray, error) {
	// Get all trips from the database
	log.Debug("Fetching all trips from the database")

	var trips models.TripArray
	err := g.db.Trips.Query(trips.Load)
	if err != nil {
		return nil, err
	}

	log.Debugf("Fetched and decoded %d trips", len(trips))

	truncated := t.Truncate(24 * time.Hour)
	nextT := t.Add(24 * time.Hour)
	weekday := truncated.Weekday()

	currentTrips := make(models.TripArray, len(trips))
	total := 0

	log.Debug("Checking each trip for current status")

	runningCache := make(map[models.Key]bool) // service id -> running
	for _, trip := range trips {
		// Get the trip start and end times
		tripStart := truncated.Add(time.Duration(trip.StartTime()) * time.Second)
		endSeconds := trip.EndTime()
		tripEnd := truncated.Add(time.Duration(endSeconds) * time.Second)

		// Adjust for midnight crossing
		tripCrossesMidnight := endSeconds > secondsInDay
		intersectsOnNextDay := false
		if tripCrossesMidnight {
			intersectsOnNextDay = nextT.After(tripStart) && nextT.Before(tripEnd)
		}

		// Check if the trip is running
		if (tripStart.After(t) || tripEnd.Before(t)) && !intersectsOnNextDay {
			continue
		}

		// Check if the trip is running on the given day
		running, ok := runningCache[trip.ServiceID]
		if !ok {
			service, err := g.GetServiceByID(trip.ServiceID)
			if err != nil {
				return nil, err
			}
			exception, _ := g.GetServiceException(trip.ServiceID, truncated)

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

// Returns all trips that are currently running
func (g *GTFS) GetAllCurrentTrips() (models.TripArray, error) {
	return g.GetCurrentTripsAt(time.Now().UTC())
}
