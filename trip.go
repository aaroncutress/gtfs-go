package gtfs

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/kelindar/column"
)

type TripDirection uint8
type TripTimepoint uint8

const (
	OutboundTripDirection TripDirection = iota
	InboundTripDirection
)
const (
	ApproximateTripTimepoint TripTimepoint = iota
	ExactTripTimepoint
)

// Represents a stop in a trip
type TripStop struct {
	StopID        Key           `json:"stop_id"`
	ArrivalTime   uint          `json:"arrival_time"`
	DepartureTime uint          `json:"departure_time"`
	Timepoint     TripTimepoint `json:"timepoint"`
}

// Converts the TripStop to a string representation
func (ts *TripStop) String() string {
	return fmt.Sprintf("%s,%d,%d,%d", ts.StopID, ts.ArrivalTime, ts.DepartureTime, ts.Timepoint)
}

// Converts a string representation back to a TripStop
func (ts *TripStop) FromString(s string) error {
	parts := strings.Split(s, ",")
	if len(parts) != 4 {
		return errors.New("invalid TripStop string format")
	}

	stopID := Key(parts[0])
	arrivalTime, err := strconv.Atoi(parts[1])
	if err != nil {
		return err
	}
	departureTime, err := strconv.Atoi(parts[2])
	if err != nil {
		return err
	}
	timepointInt, err := strconv.Atoi(parts[3])
	if err != nil {
		return err
	}

	ts.StopID = stopID
	ts.ArrivalTime = uint(arrivalTime)
	ts.DepartureTime = uint(departureTime)
	ts.Timepoint = TripTimepoint(timepointInt)
	return nil
}

type TripStopArray []*TripStop

func (tsa TripStopArray) MarshalBinary() ([]byte, error) {
	var serialized []string
	for _, stop := range tsa {
		serialized = append(serialized, stop.String())
	}
	return []byte(strings.Join(serialized, "|")), nil
}

func (tsa *TripStopArray) UnmarshalBinary(data []byte) error {
	lines := strings.Split(string(data), "|")
	stops := make([]*TripStop, 0)
	for _, line := range lines {
		stop := &TripStop{}
		err := stop.FromString(line)
		if err != nil {
			return err
		}
		stops = append(stops, stop)
	}

	*tsa = stops
	return nil
}

// Intermediate structure to hold trip stop sequences
type tripStopSequence struct {
	TripStop *TripStop
	Sequence uint
}

// Represents a trip on a particular route in a transit system
type Trip struct {
	ID        Key
	RouteID   Key
	ServiceID Key
	ShapeID   Key
	Direction TripDirection
	Headsign  string
	Stops     TripStopArray
}
type TripMap map[Key]*Trip
type TripArray []*Trip

// Saves the trip to the database
func (t Trip) Save(row column.Row) error {
	row.SetString("route_id", string(t.RouteID))
	row.SetString("service_id", string(t.ServiceID))
	row.SetString("shape_id", string(t.ShapeID))
	row.SetUint("direction", uint(t.Direction))
	row.SetString("headsign", t.Headsign)
	row.SetRecord("stops", t.Stops)

	return nil
}

// Loads the trip from the database
func (t *Trip) Load(row column.Row) error {
	key, keyOk := row.Key()
	routeID, routeIDOk := row.String("route_id")
	serviceID, serviceIDOk := row.String("service_id")
	shapeID, shapeIDOk := row.String("shape_id")
	directionInt, directionIntOk := row.Uint("direction")
	headSign, headSignOk := row.String("headsign")
	stopsAny, stopsStrOk := row.Record("stops")

	if !keyOk || !routeIDOk || !serviceIDOk || !shapeIDOk || !directionIntOk || !headSignOk || !stopsStrOk {
		return errors.New("missing required fields")
	}

	stops, ok := stopsAny.(*TripStopArray)
	if !ok {
		return errors.New("invalid stops format")
	}

	*t = Trip{
		ID:        Key(key),
		RouteID:   Key(routeID),
		ServiceID: Key(serviceID),
		ShapeID:   Key(shapeID),
		Direction: TripDirection(directionInt),
		Headsign:  headSign,
		Stops:     *stops,
	}
	return nil
}

// Loads all trips from the database transaction
func (ta *TripArray) Load(txn *column.Txn) error {
	idCol := txn.Key()
	routeIDCol := txn.String("route_id")
	serviceIDCol := txn.String("service_id")
	shapeIDCol := txn.String("shape_id")
	directionCol := txn.Uint("direction")
	headSignCol := txn.String("headsign")
	stopsCol := txn.Record("stops")

	count := txn.Count()
	if count == 0 {
		return nil
	}
	*ta = make(TripArray, count)

	var e error
	i := 0
	err := txn.Range(func(idx uint32) {
		id, idOk := idCol.Get()
		routeID, routeIDOk := routeIDCol.Get()
		serviceID, serviceIDOk := serviceIDCol.Get()
		shapeID, shapeIDOk := shapeIDCol.Get()
		directionInt, directionIntOk := directionCol.Get()
		headSign, headSignOk := headSignCol.Get()
		stopsAny, stopsStrOk := stopsCol.Get()

		if !idOk || !routeIDOk || !serviceIDOk || !shapeIDOk || !directionIntOk || !headSignOk || !stopsStrOk {
			e = errors.New("missing required fields")
			return
		}

		stops, ok := stopsAny.(*TripStopArray)
		if !ok {
			e = errors.New("invalid stops format")
			return
		}

		(*ta)[i] = &Trip{
			ID:        Key(id),
			RouteID:   Key(routeID),
			ServiceID: Key(serviceID),
			ShapeID:   Key(shapeID),
			Direction: TripDirection(directionInt),
			Headsign:  headSign,
			Stops:     *stops,
		}
		i++
	})
	if err != nil {
		return err
	}
	if e != nil {
		return e
	}
	return nil
}

// Get the time that a trip starts at the first stop
func (t *Trip) StartTime() uint {
	if len(t.Stops) == 0 {
		return 0
	}
	return t.Stops[0].ArrivalTime
}

// Get the time that a trip ends at the last stop
func (t *Trip) EndTime() uint {
	if len(t.Stops) == 0 {
		return 0
	}
	return t.Stops[len(t.Stops)-1].DepartureTime
}

// Parse time in HH:MM:SS format into seconds since midnight
func parseTime(timeStr string) (uint, error) {
	var hours, minutes, seconds uint
	_, err := fmt.Sscanf(timeStr, "%02d:%02d:%02d", &hours, &minutes, &seconds)
	if err != nil {
		return 0, err
	}
	return hours*60*60 + minutes*60 + seconds, nil
}

// Load and parse trips from the GTFS trips.txt and stop_times.txt files
func ParseTrips(tripsFile io.Reader, stopTimesFile io.Reader) (TripMap, error) {
	// Read stop_times file using CSV reader
	reader := csv.NewReader(stopTimesFile)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	tripStops := make(map[Key][]*tripStopSequence)
	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into TripStop struct
		tripID := Key(record[0])
		stopID := Key(record[3])
		arrivalTime, err := parseTime(record[1])
		if err != nil {
			return nil, err
		}
		departureTime, err := parseTime(record[2])
		if err != nil {
			return nil, err
		}

		timepointInt, err := strconv.Atoi(record[7])
		if err != nil {
			timepointInt = int(ExactTripTimepoint)
		}
		timepoint := TripTimepoint(timepointInt)

		sequenceInt, err := strconv.Atoi(record[0])
		if err != nil {
			return nil, err
		}

		if _, ok := tripStops[tripID]; !ok {
			tripStops[tripID] = make([]*tripStopSequence, 0)
		}
		tripStops[tripID] = append(tripStops[tripID], &tripStopSequence{
			TripStop: &TripStop{
				StopID:        stopID,
				ArrivalTime:   arrivalTime,
				DepartureTime: departureTime,
				Timepoint:     timepoint,
			},
			Sequence: uint(sequenceInt),
		})
	}

	// Read trips file using CSV reader
	reader = csv.NewReader(tripsFile)
	records, err = reader.ReadAll()
	if err != nil {
		return nil, err
	}

	trips := make(TripMap)
	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into Trip struct
		id := Key(record[2])
		routeID := Key(record[0])
		serviceID := Key(record[1])
		shapeID := Key(record[5])
		directionInt, err := strconv.Atoi(record[3])
		if err != nil {
			return nil, err
		}
		direction := TripDirection(directionInt)
		headSign := record[4]

		trip := &Trip{
			ID:        id,
			RouteID:   routeID,
			ServiceID: serviceID,
			ShapeID:   shapeID,
			Direction: direction,
			Headsign:  headSign,
			Stops:     make([]*TripStop, 0),
		}

		if _, ok := tripStops[id]; !ok {
			continue // skip if no stops found for this trip
		}
		tripStopSeqs := tripStops[id]
		sort.Slice(tripStopSeqs, func(i, j int) bool {
			return tripStopSeqs[i].Sequence < tripStopSeqs[j].Sequence
		})
		for _, tripStopSeq := range tripStopSeqs {
			trip.Stops = append(trip.Stops, tripStopSeq.TripStop)
		}

		trips[id] = trip
	}

	return trips, nil
}
