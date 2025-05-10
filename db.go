package gtfs

import (
	bolt "go.etcd.io/bbolt"
)

// Populates the GTFS database with data from the provided maps.
func Populate(
	db *bolt.DB,
	agencies AgencyMap,
	routes RouteMap,
	services ServiceMap,
	serviceExceptions ServiceExceptionMap,
	shapes ShapeMap,
	stops StopMap,
	trips TripMap,
) error {
	// Populate agencies
	err := db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("agencies"))
		if err != nil {
			return err
		}
		for _, agency := range agencies {
			err := b.Put([]byte(agency.ID), agency.Encode())
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Populate routes
	err = db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("routes"))
		if err != nil {
			return err
		}
		b2, err := tx.CreateBucketIfNotExists([]byte("routesByNameIndex"))
		if err != nil {
			return err
		}

		for _, route := range routes {
			err := b.Put([]byte(route.ID), route.Encode())
			if err != nil {
				return err
			}

			// Populate routesByNameIndex
			if route.Name != "" {
				err = b2.Put([]byte(route.Name), []byte(route.ID))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	// Populate services
	err = db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("services"))
		if err != nil {
			return err
		}
		for _, service := range services {
			err := b.Put([]byte(service.ID), service.Encode())
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Populate service exceptions
	err = db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("serviceExceptions"))
		if err != nil {
			return err
		}
		for _, exception := range serviceExceptions {
			id := string(exception.ServiceID) + exception.Date.Format("20060102")
			err := b.Put([]byte(id), exception.Encode())
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Populate shapes
	err = db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("shapes"))
		if err != nil {
			return err
		}
		for _, shape := range shapes {
			err := b.Put([]byte(shape.ID), shape.Encode())
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Populate stops
	err = db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("stops"))
		if err != nil {
			return err
		}
		b2, err := tx.CreateBucketIfNotExists([]byte("stopsByNameIndex"))
		if err != nil {
			return err
		}

		for _, stop := range stops {
			err := b.Put([]byte(stop.ID), stop.Encode())
			if err != nil {
				return err
			}

			// Populate stopsByNameIndex
			if stop.Name != "" {
				err = b2.Put([]byte(stop.Name), []byte(stop.ID))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	// Populate trips
	err = db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("trips"))
		if err != nil {
			return err
		}

		tripsByRouteIndex := make(map[Key]*KeyArray)
		for _, trip := range trips {
			err := b.Put([]byte(trip.ID), trip.Encode())
			if err != nil {
				return err
			}

			// Populate tripsByRouteIndex
			if trip.RouteID != "" {
				if _, exists := tripsByRouteIndex[trip.RouteID]; !exists {
					tripsByRouteIndex[trip.RouteID] = &KeyArray{}
				}
				tripsByRouteIndex[trip.RouteID].Append(trip.ID)
			}
		}

		b2, err := tx.CreateBucketIfNotExists([]byte("tripsByRouteIndex"))
		if err != nil {
			return err
		}
		for routeID, tripIDs := range tripsByRouteIndex {
			err = b2.Put([]byte(routeID), tripIDs.Encode())
			if err != nil {
				return err
			}
		}

		return nil
	})

	return nil
}
