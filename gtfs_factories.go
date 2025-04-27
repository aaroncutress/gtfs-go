package gtfs

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"github.com/hashicorp/go-set/v3"
	"resty.dev/v3"
)

const CurrentVersion = 1

// Temporary struct to hold the shape ID and stop IDs for each route
type routeShapeAndStops struct {
	shapeID Key
	stopIDs KeyArray
}
type routeShapeAndStopsMap map[Key]routeShapeAndStops

// Get the most common shape ID and stop IDs for each route
func getRouteShapeAndStops(tripMap TripMap) (routeShapeAndStopsMap, error) {
	routeTrips := make(map[Key][]*Trip)
	for _, trip := range tripMap {
		if _, ok := routeTrips[trip.RouteID]; !ok {
			routeTrips[trip.RouteID] = []*Trip{}
		}
		routeTrips[trip.RouteID] = append(routeTrips[trip.RouteID], trip)
	}

	shapeAndStops := make(routeShapeAndStopsMap)
	for routeID, trips := range routeTrips {
		inboundShapesCounts := make(map[Key]KeyArray)
		outboundShapesCounts := make(map[Key]KeyArray)

		for _, trip := range trips {
			if trip.Direction == InboundTripDirection {
				inboundShapesCounts[trip.ShapeID] = append(inboundShapesCounts[trip.ShapeID], trip.ID)
			} else {
				outboundShapesCounts[trip.ShapeID] = append(outboundShapesCounts[trip.ShapeID], trip.ID)
			}
		}

		var mostCommonInboundShapeID Key
		maxInboundCount := -1

		for shapeID, tripIDs := range inboundShapesCounts {
			if len(tripIDs) > maxInboundCount {
				maxInboundCount = len(tripIDs)
				mostCommonInboundShapeID = shapeID
			}
		}

		var mostCommonOutboundShapeID Key
		maxOutboundCount := -1

		for shapeID, tripIDs := range outboundShapesCounts {
			if len(tripIDs) > maxOutboundCount {
				maxOutboundCount = len(tripIDs)
				mostCommonOutboundShapeID = shapeID
			}
		}

		var mostCommonShapeID Key
		if maxInboundCount > maxOutboundCount {
			mostCommonShapeID = mostCommonInboundShapeID
		} else {
			mostCommonShapeID = mostCommonOutboundShapeID
		}

		if mostCommonShapeID == "" {
			continue
		}

		stopIDs := make(KeyArray, 0)

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
			stopIDs: set.From[Key](stopIDs).Slice(),
		}
	}

	return shapeAndStops, nil
}

// Load GTFS data from a local database file
func (g *GTFS) FromDB(dbFile string) error {
	log.Infof("Loading GTFS data from %s", dbFile)
	db := &gtfsdb{}
	db.initialize()
	version, err := db.load(dbFile)

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
	log.Debugf("Reading GTFS data from %s", gtfsURL)

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
	log.Debugf("Opening GTFS files from %s", gtfsURL)

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

	var agencies AgencyMap
	var routes RouteMap
	var services ServiceMap
	var serviceExceptions ServiceExceptionMap
	var shapes ShapeMap
	var stops StopMap
	var trips TripMap

	var maxShapeLength int

	var wg sync.WaitGroup
	errChannel := make(chan error, 1)
	completion := make(chan any)

	// Create functions to parse each GTFS file concurrently
	log.Debugf("Parsing GTFS data from %s", gtfsURL)

	go func() {
		for result := range completion {
			switch v := result.(type) {
			case AgencyMap:
				agencies = v
			case RouteMap:
				routes = v
			case ServiceMap:
				services = v
			case ServiceExceptionMap:
				serviceExceptions = v
			case ShapeMap:
				shapes = v
			case StopMap:
				stops = v
			case TripMap:
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
		agencies, loadErr = ParseAgencies(readers["agency.txt"])
		log.Debugf("Parsed %d agencies", len(agencies))
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
		routes, loadErr = ParseRoutes(readers["routes.txt"])
		log.Debugf("Parsed %d routes", len(routes))
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
		services, loadErr = ParseServices(readers["calendar.txt"])
		log.Debugf("Parsed %d services", len(services))
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
			log.Debugf("calendar_dates.txt not found, skipping")
			return
		}
		var loadErr error
		serviceExceptions, loadErr = ParseServiceExceptions(reader)
		log.Debugf("Parsed %d service exceptions", len(serviceExceptions))
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
			log.Debugf("shapes.txt not found, skipping")
			return
		}
		var loadErr error
		shapes, maxShapeLength, loadErr = ParseShapes(reader)
		log.Debugf("Parsed %d shapes", len(shapes))
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
		stops, loadErr = ParseStops(readers["stops.txt"])
		log.Debugf("Parsed %d stops", len(stops))
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
		trips, loadErr = ParseTrips(readers["trips.txt"], readers["stop_times.txt"])
		log.Debugf("Parsed %d trips", len(trips))
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

	log.Debugf("Finished loading GTFS data from %s", gtfsURL)

	// Get the most common shape ID and stop IDs for each route
	log.Debugf("Getting route shape and stops")

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
	log.Debugf("Creating GTFS database")
	db := &gtfsdb{}
	db.maxShapeLength = maxShapeLength
	db.initialize()

	// Populate the database with the loaded data
	log.Debugf("Populating GTFS database")
	err = db.Populate(agencies, routes, services, serviceExceptions, shapes, stops, trips)
	if err != nil {
		return err
	}

	// Save the database to the file
	g.db = db
	g.filePath = dbFile

	g.Version = CurrentVersion
	g.Created = time.Now().UTC()

	log.Debugf("Saving GTFS database to %s", dbFile)
	err = g.Save()
	if err != nil {
		return err
	}

	return nil
}
