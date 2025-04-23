package models

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
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

const secondsInDay = 60 * 60 * 24

// Represents a stop in a trip
type TripStop struct {
	StopID        Key
	ArrivalTime   uint
	DepartureTime uint
	Timepoint     TripTimepoint
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
	Stops     []*TripStop
}
type TripMap map[Key]*Trip

// Encode the Trip struct into a record
func (t *Trip) Encode() []any {
	return []any{
		string(t.ID),
		string(t.RouteID),
		string(t.ServiceID),
		string(t.ShapeID),
		int(t.Direction),
		t.Headsign,
	}
}

// Encode the Trip stops into a list of records
func (t *Trip) EncodeStops() [][]any {
	records := make([][]any, len(t.Stops))
	for i, stop := range t.Stops {
		records[i] = []any{
			string(t.ID),
			string(stop.StopID),
			i,
			stop.ArrivalTime,
			stop.DepartureTime,
			int(stop.Timepoint),
		}
	}
	return records
}

// Decode a record into a Trip struct
func DecodeTrip(record *sql.Row, tripStopRecords *sql.Rows) (*Trip, error) {
	// if len(record) < 6 {
	// 	return nil, fmt.Errorf("invalid trip record: %v", record)
	// }

	// id := Key(record[0].(string))
	// routeID := Key(record[1].(string))
	// serviceID := Key(record[2].(string))
	// shapeID := Key(record[3].(string))
	// directionInt, err := strconv.Atoi(record[4].(string))
	// if err != nil {
	// 	return nil, err
	// }
	// direction := TripDirection(directionInt)
	// headSign := record[5].(string)

	// stops := make([]*TripStop, len(tripStopRecords))
	// for _, tripStopRecord := range tripStopRecords {
	// 	if len(tripStopRecord) < 6 {
	// 		return nil, fmt.Errorf("invalid trip stop record: %v", tripStopRecord)
	// 	}
	// 	stopID := Key(tripStopRecord[1].(string))

	// 	i, err := strconv.Atoi(tripStopRecord[2].(string))
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	arrivalTime, err := strconv.Atoi(tripStopRecord[3].(string))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	departureTime, err := strconv.Atoi(tripStopRecord[4].(string))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	timepointInt, err := strconv.Atoi(tripStopRecord[5].(string))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	timepoint := TripTimepoint(timepointInt)

	// 	stops[i] = &TripStop{
	// 		StopID:        stopID,
	// 		ArrivalTime:   uint(arrivalTime),
	// 		DepartureTime: uint(departureTime),
	// 		Timepoint:     timepoint,
	// 	}
	// }

	// return &Trip{
	// 	ID:        id,
	// 	RouteID:   routeID,
	// 	ServiceID: serviceID,
	// 	ShapeID:   shapeID,
	// 	Direction: direction,
	// 	Headsign:  headSign,
	// 	Stops:     stops,
	// }, nil

	var id, routeID, serviceID, shapeID, headsign string
	var directionInt int
	err := record.Scan(&id, &routeID, &serviceID, &shapeID, &directionInt, &headsign)
	if err != nil {
		return nil, err
	}

	stops := make([]*TripStop, 0)
	sequences := make([]int, 0)

	for tripStopRecords.Next() {
		var stopID string
		var arrivalTime, departureTime uint
		var timepointInt, seq int
		err := tripStopRecords.Scan(&id, &stopID, &seq, &arrivalTime, &departureTime, &timepointInt)
		if err != nil {
			return nil, err
		}
		timepoint := TripTimepoint(timepointInt)

		stops = append(stops, &TripStop{
			StopID:        Key(stopID),
			ArrivalTime:   arrivalTime,
			DepartureTime: departureTime,
			Timepoint:     timepoint,
		})
		sequences = append(sequences, seq)
	}

	// Sort stops by sequence
	sort.Slice(stops, func(i, j int) bool {
		return sequences[i] < sequences[j]
	})

	return &Trip{
		ID:        Key(id),
		RouteID:   Key(routeID),
		ServiceID: Key(serviceID),
		ShapeID:   Key(shapeID),
		Direction: TripDirection(directionInt),
		Headsign:  headsign,
		Stops:     stops,
	}, nil
}

// Get the time that a trip starts at the first stop
func (t Trip) StartTime() uint {
	if len(t.Stops) == 0 {
		return 0
	}
	return t.Stops[0].ArrivalTime
}

// Get the time that a trip ends at the last stop
func (t Trip) EndTime() uint {
	if len(t.Stops) == 0 {
		return 0
	}
	return t.Stops[len(t.Stops)-1].DepartureTime
}

// Check if a time crosses into the next day
func IsNextDay(time uint) bool {
	return time > secondsInDay
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

// Load trips from the GTFS trips.txt and stop_times.txt files
func LoadTrips(tripsFile io.Reader, stopTimesFile io.Reader) (TripMap, error) {
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
