package gtfs

import (
	"time"

	"github.com/charmbracelet/log"
)

// Check if a given weekday is present in the flags
func hasDay(flags WeekdayFlag, day time.Weekday) bool {
	if day < time.Sunday || day > time.Saturday {
		return false
	}

	bitPos := (int(day) + 6) % 7
	dayFlag := WeekdayFlag(1 << bitPos)
	return (flags & dayFlag) != 0
}

func isTripWithinInterval(tripStartTime, tripEndTime, tSeconds, bufferSeconds int) bool {
	// Normalize trip times to potentially span beyond secondsInDay if crossing midnight
	normTripStart := tripStartTime
	normTripEnd := tripEndTime
	if tripEndTime < tripStartTime {
		normTripEnd = tripEndTime + secondsInDay
	}

	// Define the linear interval around tSeconds
	intervalStart := tSeconds - bufferSeconds
	intervalEnd := tSeconds + bufferSeconds

	// Overlap with the trip in the current window aligned with the interval
	overlapCurrent := max(intervalStart, normTripStart) <= min(intervalEnd, normTripEnd)

	// Overlap with the trip shifted back one day
	overlapPreviousDay := max(intervalStart, normTripStart-secondsInDay) <= min(intervalEnd, normTripEnd-secondsInDay)

	// Overlap with the trip shifted forward one day
	overlapNextDay := max(intervalStart, normTripStart+secondsInDay) <= min(intervalEnd, normTripEnd+secondsInDay)

	return overlapCurrent || overlapPreviousDay || overlapNextDay
}

// Returns the trips that are running at the given time with a buffer, from the given array
func (g *GTFS) GetCurrentTripsWithBuffer(trips TripMap, t time.Time, buffer time.Duration) (TripMap, error) {
	currentTrips := make(TripMap, len(trips))

	if len(trips) == 0 {
		log.Debug("No trips to check")
		return currentTrips, nil
	}

	var firstTrip *Trip
	for _, trip := range trips {
		firstTrip = trip
		break
	}

	route, err := g.GetRouteByID(firstTrip.RouteID)
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

	weekday := t.Weekday()

	runningCache := make(map[Key]bool) // service id -> running
	for tripID, trip := range trips {
		// Check if the trip is running on the current day
		running, ok := runningCache[trip.ServiceID]
		if !ok {
			service, err := g.GetServiceByID(trip.ServiceID)
			if err != nil {
				log.Errorf("Failed to get service by ID: %v", err)
				return nil, err
			}
			exception, _ := g.GetServiceException(trip.ServiceID, t)

			if exception != nil {
				running = exception.Type == AddedExceptionType
			} else {
				running = hasDay(service.Weekdays, weekday)
			}

			running = running && service.StartDate.Before(t) && service.EndDate.After(t)

			runningCache[trip.ServiceID] = running
		}

		if !running {
			continue
		}

		// Check if the trip is within the time interval
		if !isTripWithinInterval(
			int(trip.StartTime()%secondsInDay),
			int(trip.EndTime()%secondsInDay),
			int(tSeconds),
			int(buffer.Seconds())) {
			continue
		}

		currentTrips[tripID] = trip
	}

	return currentTrips, nil
}

// Returns the trips that are running at the given time from the given array
func (g *GTFS) GetCurrentTripsAt(trips TripMap, t time.Time) (TripMap, error) {
	return g.GetCurrentTripsWithBuffer(trips, t, 0)
}

// Returns the trips that are running between the given start and end times from the given array
func (g *GTFS) GetCurrentTripsBetween(trips TripMap, start, end time.Time) (TripMap, error) {
	buffer := end.Sub(start) / 2
	middle := start.Add(buffer)
	return g.GetCurrentTripsWithBuffer(trips, middle, buffer)
}

// Returns the trips that are currently running from the given array
func (g *GTFS) GetCurrentTrips(trips TripMap) (TripMap, error) {
	return g.GetCurrentTripsWithBuffer(trips, time.Now(), 0)
}

// Returns all trips that are currently running
func (g *GTFS) GetAllCurrentTrips() (TripMap, error) {
	// Fetch all trips from the GTFS database
	trips, err := g.GetAllTrips()
	if err != nil {
		return nil, err
	}

	return g.GetCurrentTripsWithBuffer(trips, time.Now(), 0)
}
