package internal

import (
	"database/sql"

	"github.com/aaroncutress/gtfs-go/models"
)

func executePragmas(db *sql.DB) {
	// Speed up inserts significantly by reducing disk syncs.
	// CAUTION: Increases risk of database corruption if the process crashes.
	// Only use if you can easily rebuild the database.
	db.Exec("PRAGMA synchronous = OFF;")

	// Use memory for the rollback journal. Faster, but journal is lost on crash.
	_, err := db.Exec("PRAGMA journal_mode = MEMORY;")
	if err != nil {
		db.Exec("PRAGMA journal_mode = WAL;")
	}

	// Increase cache size (e.g., 2GB). Adjust based on available RAM.
	// Negative value means KiB, positive means number of pages.
	db.Exec("PRAGMA cache_size = -2000000;") // -2000000 = 2,000,000 KiB = ~2GB

	// Use exclusive locking within the transaction, can reduce contention.
	db.Exec("PRAGMA locking_mode = EXCLUSIVE;")

	// Store temporary tables/indices in memory.
	db.Exec("PRAGMA temp_store = MEMORY;")
}

func PopulateDB(db *sql.DB,
	agencies models.AgencyMap,
	routes models.RouteMap,
	services models.ServiceMap,
	serviceExceptions models.ServiceExceptionMap,
	shapes models.ShapeMap,
	stops models.StopMap,
	trips models.TripMap,
) error {
	// Execute PRAGMAs to optimize performance
	executePragmas(db)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Save agencies
	agencyStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO agency (id, name, url, timezone)
		VALUES (?, ?, ?, ?);
	`)
	if err != nil {
		return err
	}
	defer agencyStmt.Close()

	for _, agency := range agencies {
		_, err := agencyStmt.Exec(agency.Encode()...)
		if err != nil {
			return err
		}
	}

	// Save routes
	routeStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO routes (id, agency_id, name, type, colour, shape_id)
		VALUES (?, ?, ?, ?, ?, ?);
	`)
	if err != nil {
		return err
	}
	defer routeStmt.Close()

	// Save route stops - prepare statement outside the loop
	routeStopStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO routes_stops (route_id, stop_id)
		VALUES (?, ?);
	`)
	if err != nil {
		return err
	}
	defer routeStopStmt.Close()

	for _, route := range routes {
		_, err := routeStmt.Exec(route.Encode()...)
		if err != nil {
			return err
		}

		// Save route stops
		stopsEncoded := route.EncodeStops()
		for _, stop := range stopsEncoded {
			_, err := routeStopStmt.Exec(stop...)
			if err != nil {
				return err
			}
		}
	}

	// Save services
	serviceStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO services (id, weekdays, start_date, end_date)
		VALUES (?, ?, ?, ?);
	`)
	if err != nil {
		return err
	}
	defer serviceStmt.Close()

	for _, service := range services {
		_, err := serviceStmt.Exec(service.Encode()...)
		if err != nil {
			return err
		}
	}

	// Save service exceptions
	serviceExceptionStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO service_exceptions (service_id, date, type)
		VALUES (?, ?, ?);
	`)
	if err != nil {
		return err
	}
	defer serviceExceptionStmt.Close()

	for _, exception := range serviceExceptions {
		_, err := serviceExceptionStmt.Exec(exception.Encode()...)
		if err != nil {
			return err
		}
	}

	// Save shapes
	shapeStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO shapes (id, sequence, coordinate)
		VALUES (?, ?, ?);
	`)
	if err != nil {
		return err
	}
	defer shapeStmt.Close()

	for _, shape := range shapes {
		shapeEncoded := shape.Encode()
		for _, record := range shapeEncoded {
			_, err := shapeStmt.Exec(record...)
			if err != nil {
				return err
			}
		}
	}

	// Save stops
	stopStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO stops (id, code, name, parent_id, location, location_type, supported_modes)
		VALUES (?, ?, ?, ?, ?, ?, ?);
	`)
	if err != nil {
		return err
	}
	defer stopStmt.Close()

	for _, stop := range stops {
		_, err := stopStmt.Exec(stop.Encode()...)
		if err != nil {
			return err
		}
	}

	// Save trips
	tripStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO trips (id, route_id, service_id, shape_id, direction, headsign)
		VALUES (?, ?, ?, ?, ?, ?);
	`)
	if err != nil {
		return err
	}
	defer tripStmt.Close()

	// Save trip stops - prepare statement outside the loop
	tripStopStmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO trip_stops (trip_id, stop_id, stop_sequence, arrival_time, departure_time, timepoint)
		VALUES (?, ?, ?, ?, ?, ?);
	`)
	if err != nil {
		return err
	}
	defer tripStopStmt.Close()

	for _, trip := range trips {
		_, err := tripStmt.Exec(trip.Encode()...)
		if err != nil {
			return err
		}

		// Save trip stops
		stopsEncoded := trip.EncodeStops()
		for _, stop := range stopsEncoded {
			_, err := tripStopStmt.Exec(stop...)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// Creates the database schema for the GTFS data
func InitializeDB(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Create agency table
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS agency (
			id TEXT PRIMARY KEY,
			name TEXT,
			url TEXT,
			timezone TEXT
		);
	`)
	if err != nil {
		return err
	}

	// Create routes table
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS routes (
			id TEXT PRIMARY KEY,
			agency_id TEXT,
			name TEXT,
			type INTEGER,
			colour TEXT,
			shape_id TEXT,

			FOREIGN KEY (agency_id) REFERENCES agency(id)
		);
	`)
	if err != nil {
		return err
	}

	// Create joining table for routes and stops
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS routes_stops (
			route_id TEXT,
			stop_id TEXT,

			PRIMARY KEY (route_id, stop_id),
			FOREIGN KEY (route_id) REFERENCES routes(id),
			FOREIGN KEY (stop_id) REFERENCES stops(id)
		);
	`)
	if err != nil {
		return err
	}

	// Create services table
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS services (
			id TEXT PRIMARY KEY,
			weekdays INTEGER,
			start_date TEXT,
			end_date TEXT
		);
	`)
	if err != nil {
		return err
	}

	// Create service exceptions table
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS service_exceptions (
			service_id TEXT,
			date TEXT,
			type INTEGER,

			PRIMARY KEY (service_id, date),
			FOREIGN KEY (service_id) REFERENCES services(id)
		);
	`)
	if err != nil {
		return err
	}

	// Create shapes table
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS shapes (
			id TEXT,
			sequence INTEGER,
			coordinate TEXT,

			PRIMARY KEY (id, sequence)
		);
	`)
	if err != nil {
		return err
	}

	// Create stops table
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS stops (
			id TEXT PRIMARY KEY,
			code TEXT,
			name TEXT,
			parent_id TEXT,
			location TEXT,
			location_type INTEGER,
			supported_modes INTEGER,

			FOREIGN KEY (parent_id) REFERENCES stops(id)
		);
	`)
	if err != nil {
		return err
	}

	// Create trips table
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS trips (
			id TEXT PRIMARY KEY,
			route_id TEXT,
			service_id TEXT,
			shape_id TEXT,
			direction INTEGER,
			headsign TEXT,

			FOREIGN KEY (route_id) REFERENCES routes(id),
			FOREIGN KEY (service_id) REFERENCES services(id),
			FOREIGN KEY (shape_id) REFERENCES shapes(id)
		);
	`)
	if err != nil {
		return err
	}

	// Create joining table for trips and stops
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS trip_stops (
			trip_id TEXT,
			stop_id TEXT,
			stop_sequence INTEGER,
			arrival_time INTEGER,
			departure_time INTEGER,
			timepoint INTEGER,

			PRIMARY KEY (trip_id, stop_id),
			FOREIGN KEY (trip_id) REFERENCES trips(id),
			FOREIGN KEY (stop_id) REFERENCES stops(id)
		);
	`)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func CreateIndices(db *sql.DB) error {
	// Create indices for the tables
	_, err := db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_routes_name ON routes (name);
		CREATE INDEX IF NOT EXISTS idx_routes_stops_route_id ON routes_stops (route_id);
		CREATE INDEX IF NOT EXISTS idx_service_exceptions_service_id ON service_exceptions (service_id);
		CREATE INDEX IF NOT EXISTS idx_shapes_id ON shapes (id);
		CREATE INDEX IF NOT EXISTS idx_stops_parent_id ON stops (parent_id);
		CREATE INDEX IF NOT EXISTS idx_trips_route_id ON trips (route_id);
		CREATE INDEX IF NOT EXISTS idx_trip_stops_trip_id ON trip_stops (trip_id);
	`)
	if err != nil {
		return err
	}

	return nil
}
