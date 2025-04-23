package gtfs

import (
	"time"

	"github.com/aaroncutress/gtfs-go/models"
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
