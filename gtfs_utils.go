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
	currentTrips := make(TripArray, len(trips))
	total := 0

	log.Debug("Checking each trip for current status")

	if len(trips) == 0 {
		log.Debug("No trips to check")
		return currentTrips[:total], nil
	}

	route, err := g.GetRouteByID(trips[0].RouteID)
	if err != nil {
		log.Errorf("Failed to get route by ID: %v", err)
		return nil, err
	}

	agency, err := g.GetAgencyByID(route.AgencyID)
	if err != nil {
		log.Errorf("Failed to get agency by ID: %v", err)
		return nil, err
	}

	timezone, err := time.LoadLocation(agency.Timezone)
	if err != nil {
		log.Errorf("Failed to load timezone: %v", err)
		return nil, err
	}

	t = t.In(timezone)
	tSeconds := t.Hour()*3600 + t.Minute()*60 + t.Second()

	intervalStart := tSeconds - int(buffer.Seconds())
	intervalEnd := tSeconds + int(buffer.Seconds())

	weekday := t.Weekday()

	runningCache := make(map[Key]bool) // service id -> running
	for _, trip := range trips {
		// Check if the trip is within the time interval
		if int(trip.StartTime()%secondsInDay) > intervalEnd ||
			int(trip.EndTime()%secondsInDay) < intervalStart {
			continue
		}

		// Check if the trip is running on the current day
		running, ok := runningCache[trip.ServiceID]
		if !ok {
			service, err := g.GetServiceByID(trip.ServiceID)
			if err != nil {
				return nil, err
			}
			exception, _ := g.GetServiceException(trip.ServiceID, t)

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
	return g.GetCurrentTripsWithBuffer(trips, time.Now(), 0)
}

// Returns all trips that are currently running
func (g *GTFS) GetAllCurrentTrips() (TripArray, error) {
	// Fetch all trips from the GTFS database
	trips, err := g.GetAllTrips()
	if err != nil {
		return nil, err
	}

	return g.GetCurrentTripsWithBuffer(trips, time.Now(), 0)
}
