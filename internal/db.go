package internal

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strconv"

	"github.com/aaroncutress/gtfs-go/models"
	"github.com/kelindar/column"
)

const CoordinatesPerRow = 2000

type GTFSDB struct {
	Agencies          *column.Collection
	Routes            *column.Collection
	Services          *column.Collection
	ServiceExceptions *column.Collection
	Shapes            *column.Collection
	Stops             *column.Collection
	Trips             *column.Collection

	// Metadata
	MaxShapeLength int
}

// Initalize the GTFS database schema
func (db *GTFSDB) Initialize() {
	// Initialize agencies
	db.Agencies = column.NewCollection()
	db.Agencies.CreateColumn("id", column.ForKey())
	db.Agencies.CreateColumn("name", column.ForString())
	db.Agencies.CreateColumn("url", column.ForString())
	db.Agencies.CreateColumn("timezone", column.ForString())

	// Initialize routes
	db.Routes = column.NewCollection()
	db.Routes.CreateColumn("id", column.ForKey())
	db.Routes.CreateColumn("agency_id", column.ForString())
	db.Routes.CreateColumn("name", column.ForString())
	db.Routes.CreateColumn("type", column.ForUint())
	db.Routes.CreateColumn("colour", column.ForString())
	db.Routes.CreateColumn("shape_id", column.ForString())
	db.Routes.CreateColumn("stops", column.ForRecord(func() *models.KeyArray {
		return new(models.KeyArray)
	}))

	// Initialize services
	db.Services = column.NewCollection()
	db.Services.CreateColumn("id", column.ForKey())
	db.Services.CreateColumn("weekdays", column.ForUint())
	db.Services.CreateColumn("start_date", column.ForString())
	db.Services.CreateColumn("end_date", column.ForString())

	// Initialize service exceptions
	db.ServiceExceptions = column.NewCollection()
	db.ServiceExceptions.CreateColumn("service_id", column.ForString())
	db.ServiceExceptions.CreateColumn("date", column.ForString())
	db.ServiceExceptions.CreateColumn("type", column.ForUint())

	// Initialize shapes
	db.Shapes = column.NewCollection()
	db.Shapes.CreateColumn("id", column.ForKey())
	numRows := int(math.Ceil(float64(db.MaxShapeLength) / float64(CoordinatesPerRow)))
	for i := range numRows {
		db.Shapes.CreateColumn("coordinates"+strconv.Itoa(i), column.ForRecord(func() *models.CoordinateArray {
			return new(models.CoordinateArray)
		}))
	}

	// Initialize stops
	db.Stops = column.NewCollection()
	db.Stops.CreateColumn("id", column.ForKey())
	db.Stops.CreateColumn("code", column.ForString())
	db.Stops.CreateColumn("name", column.ForString())
	db.Stops.CreateColumn("parent_id", column.ForString())
	db.Stops.CreateColumn("location", column.ForString())
	db.Stops.CreateColumn("location_type", column.ForUint())
	db.Stops.CreateColumn("supported_modes", column.ForUint())

	// Initialize trips
	db.Trips = column.NewCollection()
	db.Trips.CreateColumn("id", column.ForKey())
	db.Trips.CreateColumn("route_id", column.ForString())
	db.Trips.CreateColumn("service_id", column.ForString())
	db.Trips.CreateColumn("shape_id", column.ForString())
	db.Trips.CreateColumn("direction", column.ForUint())
	db.Trips.CreateColumn("headsign", column.ForString())
	db.Trips.CreateColumn("stops", column.ForRecord(func() *models.TripStopArray {
		return new(models.TripStopArray)
	}))
}

// Load loads the GTFS database from a zip file.
func (db *GTFSDB) Load(filePath string) (int, error) {
	// Initialize the database schema
	db.Initialize()

	// Open the zip file
	zipFile, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer zipFile.Close()

	fileStat, err := zipFile.Stat()
	if err != nil {
		return 0, err
	}

	// Create a new zip reader
	zipReader, err := zip.NewReader(zipFile, fileStat.Size())
	if err != nil {
		return 0, err
	}

	for _, file := range zipReader.File {
		// Open the file in the zip archive
		f, err := file.Open()
		if err != nil {
			return 0, err
		}
		defer f.Close()

		// Load the file into the appropriate collection
		switch file.Name {
		case "agencies":
			err = db.Agencies.Restore(f)
		case "routes":
			err = db.Routes.Restore(f)
		case "services":
			err = db.Services.Restore(f)
		case "service_exceptions":
			err = db.ServiceExceptions.Restore(f)
		case "shapes":
			err = db.Shapes.Restore(f)
		case "stops":
			err = db.Stops.Restore(f)
		case "trips":
			err = db.Trips.Restore(f)
		default:
			continue
		}

		if err != nil {
			return 0, err
		}
	}

	// Load the metadata file
	metadataFile, err := zipReader.Open("metadata.json")
	if err != nil {
		return 0, err
	}
	defer metadataFile.Close()

	metadata := make(map[string]any)
	err = json.NewDecoder(metadataFile).Decode(&metadata)
	if err != nil {
		return 0, err
	}

	versionF, ok := metadata["version"].(float64)
	if !ok {
		return 0, errors.New("invalid metadata version")
	}
	version := int(versionF)

	maxShapeLengthF, ok := metadata["max_shape_length"].(float64)
	if !ok {
		return 0, errors.New("invalid metadata max_shape_length")
	}
	maxShapeLength := int(maxShapeLengthF)
	db.MaxShapeLength = maxShapeLength

	return version, nil
}

// Save saves the GTFS database to a zip file.
func (db *GTFSDB) Save(filePath string, version int) error {
	zipFile, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	// Create a new zip file for each collection
	collections := map[string]*column.Collection{
		"agencies":           db.Agencies,
		"routes":             db.Routes,
		"services":           db.Services,
		"service_exceptions": db.ServiceExceptions,
		"shapes":             db.Shapes,
		"stops":              db.Stops,
		"trips":              db.Trips,
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
		"max_shape_length": db.MaxShapeLength,
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
func (db *GTFSDB) Populate(
	agencies models.AgencyMap,
	routes models.RouteMap,
	services models.ServiceMap,
	serviceExceptions models.ServiceExceptionMap,
	shapes models.ShapeMap,
	stops models.StopMap,
	trips models.TripMap,
) error {
	// Populate agencies
	for _, agency := range agencies {
		err := db.Agencies.InsertKey(string(agency.ID), agency.Save)
		if err != nil {
			return err
		}
	}

	// Populate routes
	for _, route := range routes {
		err := db.Routes.InsertKey(string(route.ID), route.Save)
		if err != nil {
			return err
		}
	}

	// Populate services
	for _, service := range services {
		err := db.Services.InsertKey(string(service.ID), service.Save)
		if err != nil {
			return err
		}
	}

	// Populate service exceptions
	for _, exception := range serviceExceptions {
		_, err := db.ServiceExceptions.Insert(exception.Save)
		if err != nil {
			return err
		}
	}

	// Populate shapes
	for _, shape := range shapes {
		err := db.Shapes.InsertKey(string(shape.ID), func(row column.Row) error {
			return shape.Save(row, CoordinatesPerRow)
		})
		if err != nil {
			return err
		}
	}

	// Populate stops
	for _, stop := range stops {
		err := db.Stops.InsertKey(string(stop.ID), stop.Save)
		if err != nil {
			return err
		}
	}

	// Populate trips
	for _, trip := range trips {
		err := db.Trips.InsertKey(string(trip.ID), trip.Save)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *GTFSDB) GetAgencies() ([]*models.Agency, error) {
	agencies := make([]*models.Agency, 0)
	err := db.Agencies.Query(func(txn *column.Txn) error {
		var err error
		agencies, err = models.LoadAllAgencies(txn)
		return err
	})
	if err != nil {
		return nil, err
	}
	return agencies, nil
}
