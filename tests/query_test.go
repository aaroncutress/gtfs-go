package tests

import (
	"testing"
	"time"
)

func TestGetAgencyByID(t *testing.T) {
	// Get the agency by ID
	agency, err := g.GetAgencyByID(agencyID)
	if err != nil {
		t.Fatalf("Failed to get agency by ID: %v", err)
	}

	// Check if the agency ID matches the expected value
	if agency.ID != agencyID {
		t.Fatalf("Expected agency ID %s, got %s", agencyID, agency.ID)
	}

	t.Logf("Agency Name: %s", agency.Name)
}

func TestGetRouteByID(t *testing.T) {
	// Get the route by ID
	route, err := g.GetRouteByID(routeID)
	if err != nil {
		t.Fatalf("Failed to get route by ID: %v", err)
	}

	// Check if the route ID matches the expected value
	if route.ID != routeID {
		t.Fatalf("Expected route ID %s, got %s", routeID, route.ID)
	}

	t.Logf("Route Name: %s", route.Name)
}

func TestGetStopByID(t *testing.T) {
	// Get the stop by ID
	stop, err := g.GetStopByID(stopID)
	if err != nil {
		t.Fatalf("Failed to get stop by ID: %v", err)
	}

	// Check if the stop ID matches the expected value
	if stop.ID != stopID {
		t.Fatalf("Expected stop ID %s, got %s", stopID, stop.ID)
	}

	t.Logf("Stop Name: %s", stop.Name)
}

func TestGetTripByID(t *testing.T) {
	// Get the trip by ID
	trip, err := g.GetTripByID(tripID)
	if err != nil {
		t.Fatalf("Failed to get trip by ID: %v", err)
	}

	// Check if the trip ID matches the expected value
	if trip.ID != tripID {
		t.Fatalf("Expected trip ID %s, got %s", tripID, trip.ID)
	}

	t.Logf("Trip Headsign: %s", trip.Headsign)
}

func TestGetTripsByRouteID(t *testing.T) {
	// Get the trips by route ID
	trips, err := g.GetTripsByRouteID(routeID)
	if err != nil {
		t.Fatalf("Failed to get trips by route ID: %v", err)
	}

	// Check if the trips are not empty
	if len(trips) == 0 {
		t.Fatal("Expected non-empty trips list")
	}

	t.Logf("Number of trips: %d", len(trips))
}

func TestGetServiceByID(t *testing.T) {
	// Get the service by ID
	service, err := g.GetServiceByID(serviceID)
	if err != nil {
		t.Fatalf("Failed to get service by ID: %v", err)
	}

	// Check if the service ID matches the expected value
	if service.ID != serviceID {
		t.Fatalf("Expected service ID %s, got %s", serviceID, service.ID)
	}

	t.Logf("Service ID: %s", service.ID)
}

func TestGetServiceException(t *testing.T) {
	// Get the service exceptions for the given date
	serviceDateParsed, err := time.Parse("2006-01-02", serviceDate)
	if err != nil {
		t.Fatalf("Failed to parse service date: %v", err)
	}

	exception, err := g.GetServiceException(serviceID, serviceDateParsed)
	if err != nil {
		t.Fatalf("Failed to get service exceptions: %v", err)
	}

	// Check if the exception ID matches the expected value
	if exception.ServiceID != serviceID {
		t.Fatalf("Expected service ID %s, got %s", serviceID, exception.ServiceID)
	}
	t.Logf("Service ID: %s", exception.ServiceID)
}

func TestGetRouteByName(t *testing.T) {
	// Get the route by name
	route, err := g.GetRouteByName(routeName)
	if err != nil {
		t.Fatalf("Failed to get route by name: %v", err)
	}

	// Check if the route name matches the expected value
	if route.Name != routeName {
		t.Fatalf("Expected route name %s, got %s", routeName, route.Name)
	}

	t.Logf("Route ID: %s", route.ID)
}

func TestGetStopByName(t *testing.T) {
	// Get the stop by name
	stop, err := g.GetStopByName(stopName)
	if err != nil {
		t.Fatalf("Failed to get stop by name: %v", err)
	}

	// Check if the stop name matches the expected value
	if stop.Name != stopName {
		t.Fatalf("Expected stop name %s, got %s", stopName, stop.Name)
	}

	t.Logf("Stop ID: %s", stop.ID)
}
