package gtfs

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aaroncutress/gtfs-go/internal"
	"github.com/aaroncutress/gtfs-go/models"
	"github.com/charmbracelet/log"
	"github.com/hashicorp/go-set/v3"
	"resty.dev/v3"
)

const CurrentVersion = 1

// Temporary struct to hold the shape ID and stop IDs for each route
type routeShapeAndStops struct {
	shapeID models.Key
	stopIDs models.KeyArray
}
type routeShapeAndStopsMap map[models.Key]routeShapeAndStops

// Get the most common shape ID and stop IDs for each route
func getRouteShapeAndStops(tripMap models.TripMap) (routeShapeAndStopsMap, error) {
	routeTrips := make(map[models.Key][]*models.Trip)
	for _, trip := range tripMap {
		if _, ok := routeTrips[trip.RouteID]; !ok {
			routeTrips[trip.RouteID] = []*models.Trip{}
		}
		routeTrips[trip.RouteID] = append(routeTrips[trip.RouteID], trip)
	}

	shapeAndStops := make(routeShapeAndStopsMap)
	for routeID, trips := range routeTrips {
		inboundShapesCounts := make(map[models.Key]models.KeyArray)
		outboundShapesCounts := make(map[models.Key]models.KeyArray)

		for _, trip := range trips {
			if trip.Direction == models.InboundTripDirection {
				inboundShapesCounts[trip.ShapeID] = append(inboundShapesCounts[trip.ShapeID], trip.ID)
			} else {
				outboundShapesCounts[trip.ShapeID] = append(outboundShapesCounts[trip.ShapeID], trip.ID)
			}
		}

		var mostCommonInboundShapeID models.Key
		maxInboundCount := -1

		for shapeID, tripIDs := range inboundShapesCounts {
			if len(tripIDs) > maxInboundCount {
				maxInboundCount = len(tripIDs)
				mostCommonInboundShapeID = shapeID
			}
		}

		var mostCommonOutboundShapeID models.Key
		maxOutboundCount := -1

		for shapeID, tripIDs := range outboundShapesCounts {
			if len(tripIDs) > maxOutboundCount {
				maxOutboundCount = len(tripIDs)
				mostCommonOutboundShapeID = shapeID
			}
		}

		var mostCommonShapeID models.Key
		if maxInboundCount > maxOutboundCount {
			mostCommonShapeID = mostCommonInboundShapeID
		} else {
			mostCommonShapeID = mostCommonOutboundShapeID
		}

		if mostCommonShapeID == "" {
			continue
		}

		stopIDs := make(models.KeyArray, 0)

		if mostCommonInboundShapeID != "" {
			for _, tripID := range inboundShapesCounts[mostCommonInboundShapeID] {
				trip, ok := tripMap[tripID]
				if !ok {
					continue
				}
				for _, stop := range trip.Stops {
					stopIDs = append(stopIDs, stop.StopID)
				}
			}
		}

		if mostCommonOutboundShapeID != "" {
			for _, tripID := range outboundShapesCounts[mostCommonOutboundShapeID] {
				trip, ok := tripMap[tripID]
				if !ok {
					continue
				}
				for _, stop := range trip.Stops {
					stopIDs = append(stopIDs, stop.StopID)
				}
			}
		}

		shapeAndStops[routeID] = routeShapeAndStops{
			shapeID: mostCommonShapeID,
			stopIDs: set.From[models.Key](stopIDs).Slice(),
		}
	}

	return shapeAndStops, nil
}

// Load GTFS data from a local database file
func (g *GTFS) FromDB(dbFile string) error {
	log.Infof("Loading GTFS data from %s", dbFile)
	db := &internal.GTFSDB{}
	db.Initialize()
	version, err := db.Load(dbFile)

	if err != nil {
		return err
	}
	g.db = db
	g.filePath = dbFile
	g.Version = version

	return nil
}

// Construct a new GTFS database from a hosted GTFS URL
func (g *GTFS) FromURL(gtfsURL, dbFile string) error {
	// Delete the existing database file if it exists
	if _, err := os.Stat(dbFile); err == nil {
		err = os.Remove(dbFile)
		if err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	// Create the database file
	dirPath := filepath.Dir(dbFile)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}
	_, err = os.Create(dbFile)
	if err != nil {
		return err
	}

	// Download the GTFS data from the URL
	log.Infof("Downloading GTFS data from %s", gtfsURL)

	client := resty.New()
	defer client.Close()

	resp, err := client.R().Get(gtfsURL)
	if err != nil {
		return err
	}
	if resp.IsError() {
		return errors.New("failed to download GTFS data: " + resp.Status())
	}

	// Read the zip file from the response body
	log.Infof("Reading GTFS data from %s", gtfsURL)

	zipBytes, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return err
	}
	zipReader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return err
	}

	// Open all files in the zip archive
	log.Infof("Opening GTFS files from %s", gtfsURL)

	readers := make(map[string]io.Reader)
	openFiles := []io.ReadCloser{}

	for _, file := range zipReader.File {
		f, err := file.Open()
		if err != nil {
			return err
		}
		defer f.Close()

		openFiles = append(openFiles, f)
		readers[file.Name] = f
	}

	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	// Check for required files
	for _, file := range requiredFiles {
		if _, ok := readers[file]; !ok {
			return errors.New("missing required GTFS file: " + file)
		}
	}

	var agencies models.AgencyMap
	var routes models.RouteMap
	var services models.ServiceMap
	var serviceExceptions models.ServiceExceptionMap
	var shapes models.ShapeMap
	var stops models.StopMap
	var trips models.TripMap

	var maxShapeLength int

	var wg sync.WaitGroup
	errChannel := make(chan error, 1)
	completion := make(chan any)

	// Create functions to load each GTFS file concurrently
	log.Infof("Loading GTFS data from %s", gtfsURL)

	go func() {
		for result := range completion {
			switch v := result.(type) {
			case models.AgencyMap:
				agencies = v
			case models.RouteMap:
				routes = v
			case models.ServiceMap:
				services = v
			case models.ServiceExceptionMap:
				serviceExceptions = v
			case models.ShapeMap:
				shapes = v
			case models.StopMap:
				stops = v
			case models.TripMap:
				trips = v
			case int:
				maxShapeLength = v
			}
		}
	}()

	// Load agencies
	wg.Add(1)
	go func() {
		defer wg.Done()
		var loadErr error // Declare err within this scope
		agencies, loadErr = models.ParseAgencies(readers["agency.txt"])
		log.Infof("Loaded %d agencies", len(agencies))
		if loadErr != nil {
			select { // Non-blocking send to avoid deadlock if errChan is full
			case errChannel <- loadErr:
			default:
			}
			return
		}
		completion <- agencies
	}()

	// Load routes
	wg.Add(1)
	go func() {
		defer wg.Done()
		var loadErr error
		routes, loadErr = models.ParseRoutes(readers["routes.txt"])
		log.Infof("Loaded %d routes", len(routes))
		if loadErr != nil {
			select {
			case errChannel <- loadErr:
			default:
			}
			return
		}
		completion <- routes
	}()

	// Load services (calendar.txt)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var loadErr error
		services, loadErr = models.ParseServices(readers["calendar.txt"])
		log.Infof("Loaded %d services", len(services))
		if loadErr != nil {
			select {
			case errChannel <- loadErr:
			default:
			}
			return
		}
		completion <- services
	}()

	// Load service exceptions (calendar_dates.txt) - Optional file
	wg.Add(1)
	go func() {
		defer wg.Done()
		reader, ok := readers["calendar_dates.txt"]
		if !ok {
			// File not found, just exit the goroutine. wg.Done() handles the counter.
			return
		}
		var loadErr error
		serviceExceptions, loadErr = models.ParseServiceExceptions(reader)
		log.Infof("Loaded %d service exceptions", len(serviceExceptions))
		if loadErr != nil {
			select {
			case errChannel <- loadErr:
			default:
			}
			return
		}
		completion <- serviceExceptions
	}()

	// Load shapes (shapes.txt) - Optional file
	wg.Add(1)
	go func() {
		defer wg.Done()
		reader, ok := readers["shapes.txt"]
		if !ok {
			// File not found, just exit the goroutine. wg.Done() handles the counter.
			return
		}
		var loadErr error
		shapes, maxShapeLength, loadErr = models.ParseShapes(reader)
		log.Infof("Loaded %d shapes", len(shapes))
		if loadErr != nil {
			select {
			case errChannel <- loadErr:
			default:
			}
			return
		}

		completion <- shapes
		completion <- maxShapeLength
	}()

	// Load stops
	wg.Add(1)
	go func() {
		defer wg.Done()
		var loadErr error
		stops, loadErr = models.ParseStops(readers["stops.txt"])
		log.Infof("Loaded %d stops", len(stops))
		if loadErr != nil {
			select {
			case errChannel <- loadErr:
			default:
			}
			return
		}
		completion <- stops
	}()

	// Load trips (trips.txt and stop_times.txt)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var loadErr error
		trips, loadErr = models.ParseTrips(readers["trips.txt"], readers["stop_times.txt"])
		log.Infof("Loaded %d trips", len(trips))
		if loadErr != nil {
			select {
			case errChannel <- loadErr:
			default:
			}
			return
		}
		completion <- trips
	}()

	wg.Wait()
	close(completion)
	defer close(errChannel)

	select {
	case err := <-errChannel:
		if err != nil {
			return err
		}
	default:
	}

	log.Infof("Finished loading GTFS data from %s", gtfsURL)

	// Get the most common shape ID and stop IDs for each route
	log.Infof("Getting route shape and stops")

	shapeAndStops, err := getRouteShapeAndStops(trips)
	if err != nil {
		return err
	}
	for routeID, shapeAndStopsData := range shapeAndStops {
		route, ok := routes[routeID]
		if !ok {
			continue
		}
		route.ShapeID = shapeAndStopsData.shapeID
		route.Stops = shapeAndStopsData.stopIDs
		routes[routeID] = route
	}

	// Create the GTFS database
	log.Infof("Creating GTFS database")
	db := &internal.GTFSDB{}
	db.MaxShapeLength = maxShapeLength
	db.Initialize()

	// Populate the database with the loaded data
	log.Infof("Populating GTFS database")
	err = db.Populate(agencies, routes, services, serviceExceptions, shapes, stops, trips)
	if err != nil {
		return err
	}

	// Save the database to the file
	g.db = db
	g.filePath = dbFile
	g.Version = CurrentVersion

	log.Infof("Saving GTFS database to %s", dbFile)
	err = g.Save()
	if err != nil {
		return err
	}

	return nil
}
