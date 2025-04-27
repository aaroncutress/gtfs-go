package gtfs

import (
	"time"

	"github.com/charmbracelet/log"
)

const secondsInDay = 24 * 60 * 60

// Check if a given weekday is present in the flags
func hasDay(flags WeekdayFlag, day time.Weekday) bool {
	if day < time.Sunday || day > time.Saturday {
		return false
	}

	bitPos := (int(day) + 6) % 7
	dayFlag := WeekdayFlag(1 << bitPos)
	return (flags & dayFlag) != 0
}

// Returns the trips that are running at the given time with a buffer, from the given array
func (g *GTFS) GetCurrentTripsWithBuffer(trips TripArray, t time.Time, buffer time.Duration) (TripArray, error) {
	truncated := t.Truncate(24 * time.Hour)
	nextT := t.Add(24 * time.Hour)
	weekday := truncated.Weekday()

	currentTrips := make(TripArray, len(trips))
	intervalStart := t.Add(-buffer)
	intervalEnd := t.Add(buffer)
	total := 0

	log.Debug("Checking each trip for current status")

	runningCache := make(map[Key]bool) // service id -> running
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

		// Check if the trip is running in the buffered time
		if !(intervalStart.Before(tripEnd) && intervalEnd.After(tripStart)) && !intersectsOnNextDay {
			continue
		}

		// Check if the trip is running on the current day
		running, ok := runningCache[trip.ServiceID]
		if !ok {
			service, err := g.GetServiceByID(trip.ServiceID)
			if err != nil {
				return nil, err
			}
			exception, _ := g.GetServiceException(trip.ServiceID, truncated)

			if hasDay(service.Weekdays, weekday) {
				running = exception == nil || exception.Type != RemovedExceptionType
			} else {
				running = exception != nil && exception.Type == AddedExceptionType
			}

			runningCache[trip.ServiceID] = running
		}

		if !running {
			continue
		}

		currentTrips[total] = trip
		total++
	}

	currentTrips = currentTrips[:total]
	return currentTrips, nil
}

// Returns the trips that are running at the given time from the given array
func (g *GTFS) GetCurrentTripsAt(trips TripArray, t time.Time) (TripArray, error) {
	return g.GetCurrentTripsWithBuffer(trips, t, 0)
}

// Returns the trips that are currently running from the given array
func (g *GTFS) GetCurrentTrips(trips TripArray) (TripArray, error) {
	return g.GetCurrentTripsWithBuffer(trips, time.Now().UTC(), 0)
}

// Returns all trips that are currently running
func (g *GTFS) GetAllCurrentTrips() (TripArray, error) {
	// Fetch all trips from the GTFS database
	trips, err := g.GetAllTrips()
	if err != nil {
		return nil, err
	}

	return g.GetCurrentTripsWithBuffer(trips, time.Now().UTC(), 0)
}
