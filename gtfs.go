package gtfs

import (
	"errors"
	"time"

	"github.com/aaroncutress/gtfs-go/internal"
	"github.com/aaroncutress/gtfs-go/models"
	"github.com/kelindar/column"
)

var requiredFiles = []string{
	"agency.txt",
	"calendar.txt",
	"stops.txt",
	"routes.txt",
	"trips.txt",
	"stop_times.txt",
}

// Represents a GTFS database connection
type GTFS struct {
	Version int

	filePath string
	db       *internal.GTFSDB
}

// Save the GTFS database to the file
func (g *GTFS) Save() error {
	return g.db.Save(g.filePath, g.Version)
}

// Returns the agency with the given ID
func (g *GTFS) GetAgencyByID(agencyID models.Key) (*models.Agency, error) {
	var agency *models.Agency

	// Query the database for the agency with the given ID
	err := g.db.Agencies.QueryKey(string(agencyID), func(row column.Row) error {
		agency = &models.Agency{}
		if err := agency.Load(row); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return agency, nil
}

// Returns the agency for a given route ID
func (g *GTFS) GetAgencyByRouteID(routeID models.Key) (*models.Agency, error) {
	var agencyID string

	// Query the database for the agency ID associated with the route
	err := g.db.Routes.QueryKey(string(routeID), func(row column.Row) error {
		var ok bool
		agencyID, ok = row.String("agency_id")
		if !ok {
			return errors.New("missing agency_id")
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Query the database for the agency with the given ID
	return g.GetAgencyByID(models.Key(agencyID))
}

// Returns the route with the given ID
func (g *GTFS) GetRouteByID(routeID models.Key) (*models.Route, error) {
	var route *models.Route

	// Query the database for the route with the given ID
	err := g.db.Routes.QueryKey(string(routeID), func(row column.Row) error {
		route = &models.Route{}
		if err := route.Load(row); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return route, nil
}

// Returns the agency with the given ID
func (g *GTFS) GetStopByID(stopID models.Key) (*models.Stop, error) {
	var stop *models.Stop

	// Query the database for the stop with the given ID
	err := g.db.Stops.QueryKey(string(stopID), func(row column.Row) error {
		stop = &models.Stop{}
		if err := stop.Load(row); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return stop, nil
}

func (g *GTFS) GetTripByID(tripID models.Key) (*models.Trip, error) {
	var trip *models.Trip

	// Query the database for the trip with the given ID
	err := g.db.Trips.QueryKey(string(tripID), func(row column.Row) error {
		trip = &models.Trip{}
		if err := trip.Load(row); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return trip, nil
}

// Returns all trips for a given route ID
func (g *GTFS) GetTripsByRouteID(routeID models.Key) ([]*models.Trip, error) {
	var trips []*models.Trip

	// Query the database for all trips associated with the route ID
	err := g.db.Trips.Query(func(txn *column.Txn) error {
		txnFilter := txn.WithValue("route_id", func(v any) bool {
			return v == string(routeID)
		})

		var err error
		trips, err = models.LoadAllTrips(txnFilter)
		return err
	})

	if err != nil {
		return nil, err
	}
	return trips, nil
}

// Returns the service with the given ID
func (g *GTFS) GetServiceByID(serviceID models.Key) (*models.Service, error) {
	var service *models.Service

	// Query the database for the service with the given ID
	err := g.db.Services.QueryKey(string(serviceID), func(row column.Row) error {
		service = &models.Service{}
		if err := service.Load(row); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return service, nil
}

// Returns all services exceptions for a given service ID
func (g *GTFS) GetServiceExceptionsByServiceID(serviceID models.Key) ([]*models.ServiceException, error) {
	var exceptions []*models.ServiceException

	// Query the database for all service exceptions associated with the service ID
	err := g.db.ServiceExceptions.Query(func(txn *column.Txn) error {
		var e error

		idCol := txn.String("service_id")
		dateCol := txn.String("date")
		typeIntCol := txn.Uint("type")

		txn.WithValue("service_id", func(v any) bool {
			return v == string(serviceID)
		}).Range(func(i uint32) {
			id, idOk := idCol.Get()
			date, dateOk := dateCol.Get()
			typeInt, typeIntOk := typeIntCol.Get()

			if !idOk || !dateOk || !typeIntOk {
				e = errors.New("missing required fields")
				return
			}

			// Parse the date string into a time.Time object
			dateTime, err := time.Parse("20060102", date)
			if err != nil {
				e = errors.New("failed to parse date")
				return
			}

			exception := &models.ServiceException{
				ServiceID: models.Key(id),
				Date:      dateTime,
				Type:      models.ExceptionType(typeInt),
			}

			exceptions = append(exceptions, exception)
		})

		return e
	})

	if err != nil {
		return nil, err
	}
	return exceptions, nil
}

// Returns all agencies in the GTFS database
func (g *GTFS) GetAllAgencies() ([]*models.Agency, error) {
	agencies := make([]*models.Agency, 0)
	err := g.db.Agencies.Query(func(txn *column.Txn) error {
		var err error
		agencies, err = models.LoadAllAgencies(txn)

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return agencies, nil
}
