package gtfs

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strconv"

	"github.com/kelindar/column"
)

const CoordinatesPerRow = 2000

type gtfsdb struct {
	// GTFS database collections
	agencies          *column.Collection
	routes            *column.Collection
	services          *column.Collection
	serviceExceptions *column.Collection
	shapes            *column.Collection
	stops             *column.Collection
	trips             *column.Collection

	// Index map collections
	routesByNameIndex *column.Collection
	stopsByNameIndex  *column.Collection
	tripsByRouteIndex *column.Collection

	// Metadata
	maxShapeLength int
}

// Initalize the GTFS database schema
func (db *gtfsdb) initialize() {
	// Initialize agencies
	db.agencies = column.NewCollection()
	db.agencies.CreateColumn("id", column.ForKey())
	db.agencies.CreateColumn("name", column.ForString())
	db.agencies.CreateColumn("url", column.ForString())
	db.agencies.CreateColumn("timezone", column.ForString())

	// Initialize routes
	db.routes = column.NewCollection()
	db.routes.CreateColumn("id", column.ForKey())
	db.routes.CreateColumn("agency_id", column.ForString())
	db.routes.CreateColumn("name", column.ForString())
	db.routes.CreateColumn("type", column.ForUint())
	db.routes.CreateColumn("colour", column.ForString())
	db.routes.CreateColumn("shape_id", column.ForString())
	db.routes.CreateColumn("stops", column.ForRecord(func() *KeyArray {
		return new(KeyArray)
	}))

	// Initialize services
	db.services = column.NewCollection()
	db.services.CreateColumn("id", column.ForKey())
	db.services.CreateColumn("weekdays", column.ForUint())
	db.services.CreateColumn("start_date", column.ForString())
	db.services.CreateColumn("end_date", column.ForString())

	// Initialize service exceptions
	db.serviceExceptions = column.NewCollection()
	db.serviceExceptions.CreateColumn("id_date", column.ForKey())
	db.serviceExceptions.CreateColumn("type", column.ForUint())

	// Initialize shapes
	db.shapes = column.NewCollection()
	db.shapes.CreateColumn("id", column.ForKey())
	numRows := int(math.Ceil(float64(db.maxShapeLength) / float64(CoordinatesPerRow)))
	for i := range numRows {
		db.shapes.CreateColumn("coordinates"+strconv.Itoa(i), column.ForRecord(func() *CoordinateArray {
			return new(CoordinateArray)
		}))
	}

	// Initialize stops
	db.stops = column.NewCollection()
	db.stops.CreateColumn("id", column.ForKey())
	db.stops.CreateColumn("code", column.ForString())
	db.stops.CreateColumn("name", column.ForString())
	db.stops.CreateColumn("parent_id", column.ForString())
	db.stops.CreateColumn("location", column.ForString())
	db.stops.CreateColumn("location_type", column.ForUint())
	db.stops.CreateColumn("supported_modes", column.ForUint())

	// Initialize trips
	db.trips = column.NewCollection()
	db.trips.CreateColumn("id", column.ForKey())
	db.trips.CreateColumn("route_id", column.ForString())
	db.trips.CreateColumn("service_id", column.ForString())
	db.trips.CreateColumn("shape_id", column.ForString())
	db.trips.CreateColumn("direction", column.ForUint())
	db.trips.CreateColumn("headsign", column.ForString())
	db.trips.CreateColumn("stops", column.ForRecord(func() *TripStopArray {
		return new(TripStopArray)
	}))

	// --- Index Collections ---

	// Initialize routesByNameIndex
	db.routesByNameIndex = column.NewCollection()
	db.routesByNameIndex.CreateColumn("name", column.ForKey())
	db.routesByNameIndex.CreateColumn("ids", column.ForRecord(func() *KeyArray {
		return new(KeyArray)
	}))

	// Initialize stopsByNameIndex
	db.stopsByNameIndex = column.NewCollection()
	db.stopsByNameIndex.CreateColumn("name", column.ForKey())
	db.stopsByNameIndex.CreateColumn("ids", column.ForRecord(func() *KeyArray {
		return new(KeyArray)
	}))

	// Initialize tripsByRouteIndex
	db.tripsByRouteIndex = column.NewCollection()
	db.tripsByRouteIndex.CreateColumn("route_id", column.ForKey())
	db.tripsByRouteIndex.CreateColumn("ids", column.ForRecord(func() *KeyArray {
		return new(KeyArray)
	}))
}

// load loads the GTFS database from a zip file.
func (db *gtfsdb) load(filePath string) (int, int64, error) {
	// Initialize the database schema
	db.initialize()

	// Open the zip file
	zipFile, err := os.Open(filePath)
	if err != nil {
		return 0, 0, err
	}
	defer zipFile.Close()

	fileStat, err := zipFile.Stat()
	if err != nil {
		return 0, 0, err
	}

	// Create a new zip reader
	zipReader, err := zip.NewReader(zipFile, fileStat.Size())
	if err != nil {
		return 0, 0, err
	}

	for _, file := range zipReader.File {
		// Open the file in the zip archive
		f, err := file.Open()
		if err != nil {
			return 0, 0, err
		}
		defer f.Close()

		// Load the file into the appropriate collection
		switch file.Name {
		case "agencies":
			err = db.agencies.Restore(f)
		case "routes":
			err = db.routes.Restore(f)
		case "services":
			err = db.services.Restore(f)
		case "service_exceptions":
			err = db.serviceExceptions.Restore(f)
		case "shapes":
			err = db.shapes.Restore(f)
		case "stops":
			err = db.stops.Restore(f)
		case "trips":
			err = db.trips.Restore(f)
		case "routes_by_name_index":
			err = db.routesByNameIndex.Restore(f)
		case "stops_by_name_index":
			err = db.stopsByNameIndex.Restore(f)
		case "trips_by_route_index":
			err = db.tripsByRouteIndex.Restore(f)
		default:
			continue
		}

		if err != nil {
			return 0, 0, err
		}
	}

	// Load the metadata file
	metadataFile, err := zipReader.Open("metadata.json")
	if err != nil {
		return 0, 0, err
	}
	defer metadataFile.Close()

	metadata := make(map[string]any)
	err = json.NewDecoder(metadataFile).Decode(&metadata)
	if err != nil {
		return 0, 0, err
	}

	versionF, ok := metadata["version"].(float64)
	if !ok {
		return 0, 0, errors.New("invalid metadata version")
	}
	version := int(versionF)

	createdF, ok := metadata["created"].(float64)
	if !ok {
		return 0, 0, errors.New("invalid metadata created")
	}
	created := int64(createdF)

	maxShapeLengthF, ok := metadata["max_shape_length"].(float64)
	if !ok {
		return 0, 0, errors.New("invalid metadata max_shape_length")
	}
	maxShapeLength := int(maxShapeLengthF)
	db.maxShapeLength = maxShapeLength

	return version, created, nil
}

// save saves the GTFS database to a zip file.
func (db *gtfsdb) save(filePath string, version int, created int64) error {
	zipFile, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	// Create a new zip file for each collection
	collections := map[string]*column.Collection{
		"agencies":             db.agencies,
		"routes":               db.routes,
		"services":             db.services,
		"service_exceptions":   db.serviceExceptions,
		"shapes":               db.shapes,
		"stops":                db.stops,
		"trips":                db.trips,
		"routes_by_name_index": db.routesByNameIndex,
		"stops_by_name_index":  db.stopsByNameIndex,
		"trips_by_route_index": db.tripsByRouteIndex,
	}

	// Write each collection to a separate file in the zip archive
	for name, collection := range collections {
		file, err := zipWriter.Create(name)
		if err != nil {
			return err
		}

		err = collection.Snapshot(file)
		if err != nil {
			return err
		}
	}

	// Write the metadata file
	metadataFile, err := zipWriter.Create("metadata.json")
	if err != nil {
		return err
	}
	metadata := map[string]any{
		"version":          version,
		"created":          created,
		"max_shape_length": db.maxShapeLength,
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	_, err = metadataFile.Write(metadataJSON)
	if err != nil {
		return err
	}

	return nil
}

// Populates the GTFS database with data from the provided maps.
func (db *gtfsdb) Populate(
	agencies AgencyMap,
	routes RouteMap,
	services ServiceMap,
	serviceExceptions ServiceExceptionMap,
	shapes ShapeMap,
	stops StopMap,
	trips TripMap,
) error {
	// Create index maps
	routesByNameIndex := make(map[string]*KeyArray)
	stopsByNameIndex := make(map[string]*KeyArray)
	tripsByRouteIndex := make(map[Key]*KeyArray)

	// Populate agencies
	db.agencies.Query(func(txn *column.Txn) error {
		for _, agency := range agencies {
			err := txn.InsertKey(string(agency.ID), agency.Save)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Populate routes
	db.routes.Query(func(txn *column.Txn) error {
		for _, route := range routes {
			err := txn.InsertKey(string(route.ID), route.Save)
			if err != nil {
				return err
			}

			// Populate routesByNameIndex
			if route.Name != "" {
				if _, exists := routesByNameIndex[route.Name]; !exists {
					routesByNameIndex[route.Name] = new(KeyArray)
				}
				routesByNameIndex[route.Name].Append(route.ID)
			}
		}
		return nil
	})

	// Populate services
	db.services.Query(func(txn *column.Txn) error {
		for _, service := range services {
			err := txn.InsertKey(string(service.ID), service.Save)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Populate service exceptions
	db.serviceExceptions.Query(func(txn *column.Txn) error {
		for _, exception := range serviceExceptions {
			key := string(exception.ServiceID) + exception.Date.Format("20060102")
			err := txn.InsertKey(key, exception.Save)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Populate shapes
	db.shapes.Query(func(txn *column.Txn) error {
		for _, shape := range shapes {
			err := txn.InsertKey(string(shape.ID), func(row column.Row) error {
				return shape.Save(row, CoordinatesPerRow)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Populate stops
	db.stops.Query(func(txn *column.Txn) error {
		for _, stop := range stops {
			err := txn.InsertKey(string(stop.ID), stop.Save)
			if err != nil {
				return err
			}

			// Populate stopsByNameIndex
			if stop.Name != "" {
				if _, exists := stopsByNameIndex[stop.Name]; !exists {
					stopsByNameIndex[stop.Name] = new(KeyArray)
				}
				stopsByNameIndex[stop.Name].Append(stop.ID)
			}
		}
		return nil
	})

	// Populate trips
	db.trips.Query(func(txn *column.Txn) error {
		for _, trip := range trips {
			err := txn.InsertKey(string(trip.ID), trip.Save)
			if err != nil {
				return err
			}

			// Populate tripsByRouteIndex
			if trip.RouteID != "" {
				if _, exists := tripsByRouteIndex[trip.RouteID]; !exists {
					tripsByRouteIndex[trip.RouteID] = new(KeyArray)
				}
				tripsByRouteIndex[trip.RouteID].Append(trip.ID)
			}
		}
		return nil
	})

	// Populate index collections
	db.routesByNameIndex.Query(func(txn *column.Txn) error {
		for name, ids := range routesByNameIndex {
			err := txn.InsertKey(name, func(row column.Row) error {
				row.SetRecord("ids", ids)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	db.stopsByNameIndex.Query(func(txn *column.Txn) error {
		for name, ids := range stopsByNameIndex {
			err := txn.InsertKey(name, func(row column.Row) error {
				row.SetRecord("ids", ids)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	db.tripsByRouteIndex.Query(func(txn *column.Txn) error {
		for routeID, ids := range tripsByRouteIndex {
			err := txn.InsertKey(string(routeID), func(row column.Row) error {
				row.SetRecord("ids", ids)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return nil
}
