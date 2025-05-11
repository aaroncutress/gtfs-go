package gtfs

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
)

type TripDirection bool
type TripTimepoint bool

const (
	OutboundTripDirection TripDirection = false
	InboundTripDirection  TripDirection = true
)
const (
	ApproximateTripTimepoint TripTimepoint = false
	ExactTripTimepoint       TripTimepoint = true
)

// Represents a stop in a trip
type TripStop struct {
	StopID        Key           `json:"stop_id"`
	ArrivalTime   uint          `json:"arrival_time"`
	DepartureTime uint          `json:"departure_time"`
	Timepoint     TripTimepoint `json:"timepoint"`
}

// Encodes the TripStop struct into a byte slice
// Format:
// - StopID: 4-byte length + UTF-8 string
// - ArrivalTime: 4 bytes (uint32)
// - DepartureTime: 4 bytes (uint32)
// - Timepoint: 1 byte (bool as uint8)
func (ts *TripStop) Encode() []byte {
	stopIDStr := string(ts.StopID)

	// Calculate total length
	totalLen := lenBytes + len(stopIDStr) + // StopID
		uint32Bytes + // ArrivalTime
		uint32Bytes + // DepartureTime
		boolBytes // Timepoint

	data := make([]byte, totalLen)
	offset := 0

	// Marshal StopID
	binary.BigEndian.PutUint32(data[offset:], uint32(len(stopIDStr)))
	offset += lenBytes
	copy(data[offset:], stopIDStr)
	offset += len(stopIDStr)

	// Marshal ArrivalTime (as uint32)
	binary.BigEndian.PutUint32(data[offset:], uint32(ts.ArrivalTime))
	offset += uint32Bytes

	// Marshal DepartureTime (as uint32)
	binary.BigEndian.PutUint32(data[offset:], uint32(ts.DepartureTime))
	offset += uint32Bytes

	// Marshal Timepoint (bool as uint8)
	if ts.Timepoint {
		data[offset] = 1
	} else {
		data[offset] = 0
	}

	return data
}

// Decodes the byte slice into the TripStop struct
func (ts *TripStop) Decode(data []byte) error {
	if ts == nil {
		return errors.New("cannot decode into a nil TripStop")
	}
	offset := 0

	// Unmarshal StopID
	if offset+lenBytes > len(data) {
		return errors.New("tripstop buffer too small for StopID length")
	}
	stopIDLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(stopIDLen) > len(data) {
		return errors.New("tripstop buffer too small for StopID content")
	}
	ts.StopID = Key(data[offset : offset+int(stopIDLen)])
	offset += int(stopIDLen)

	// Unmarshal ArrivalTime
	if offset+uint32Bytes > len(data) {
		return errors.New("tripstop buffer too small for ArrivalTime")
	}
	ts.ArrivalTime = uint(binary.BigEndian.Uint32(data[offset:]))
	offset += uint32Bytes

	// Unmarshal DepartureTime
	if offset+uint32Bytes > len(data) {
		return errors.New("tripstop buffer too small for DepartureTime")
	}
	ts.DepartureTime = uint(binary.BigEndian.Uint32(data[offset:]))
	offset += uint32Bytes

	// Unmarshal Timepoint
	if offset+boolBytes > len(data) {
		return errors.New("tripstop buffer too small for Timepoint")
	}
	if data[offset] == 1 {
		ts.Timepoint = true
	} else if data[offset] == 0 {
		ts.Timepoint = false
	} else {
		return fmt.Errorf("invalid byte value for bool (Timepoint): got %d, want 0 or 1", data[offset])
	}
	offset += boolBytes

	// Check if all data was consumed
	if offset != len(data) {
		return errors.New("tripstop buffer not fully consumed, trailing data exists")
	}
	return nil
}

type TripStopArray []*TripStop

// Encode the TripStopArray into a byte slice
// Format:
// - Count: 4 bytes (uint32)
// - Each TripStop (see TripStop.Encode)
func (tsa TripStopArray) Encode() []byte {
	var totalLen int = lenBytes // Start with count length
	var encodedStops [][]byte   // Store individually encoded stops to avoid re-encoding

	for _, ts := range tsa {
		tripStopBytes := ts.Encode()
		encodedStops = append(encodedStops, tripStopBytes)
		totalLen += lenBytes + len(tripStopBytes)
	}

	data := make([]byte, totalLen)
	offset := 0

	// Marshal count
	binary.BigEndian.PutUint32(data[offset:], uint32(len(tsa))) // Use original length of tsa
	offset += lenBytes

	// Marshal each TripStop
	for _, tripStopBytes := range encodedStops {
		binary.BigEndian.PutUint32(data[offset:], uint32(len(tripStopBytes)))
		offset += lenBytes
		copy(data[offset:], tripStopBytes)
		offset += len(tripStopBytes)
	}
	return data
}

// Decode the byte slice into the TripStopArray
func (tsa *TripStopArray) Decode(data []byte) error {
	if tsa == nil {
		return errors.New("cannot decode into a nil TripStopArray")
	}
	offset := 0

	// Unmarshal count
	if offset+lenBytes > len(data) {
		return errors.New("tripstoparray buffer too small for count")
	}
	count := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes

	// Unmarshal TripStops
	tempTsa := make(TripStopArray, count)
	for i := uint32(0); i < count; i++ {
		// Unmarshal length of the current TripStop's data
		if offset+lenBytes > len(data) {
			return fmt.Errorf("tripstoparray buffer too small for TripStop %d data length", i)
		}
		tripStopDataLen := binary.BigEndian.Uint32(data[offset:])
		offset += lenBytes

		// Unmarshal the TripStop's data
		if offset+int(tripStopDataLen) > len(data) {
			return fmt.Errorf("tripstoparray buffer too small for TripStop %d content (expected %d bytes)", i, tripStopDataLen)
		}

		currentTripStopData := data[offset : offset+int(tripStopDataLen)]

		var tripStop TripStop                       // Create a value
		err := tripStop.Decode(currentTripStopData) // Decode into the value
		if err != nil {
			return fmt.Errorf("failed to decode TripStop %d: %w", i, err)
		}
		tempTsa[i] = &tripStop // Store pointer to the decoded value
		offset += int(tripStopDataLen)
	}
	*tsa = tempTsa

	// Check if all data was consumed
	if offset != len(data) {
		return errors.New("tripstoparray buffer not fully consumed, trailing data exists")
	}
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

// Encode the Trip struct into a byte slice
// Format:
// - RouteID: 4-byte length + UTF-8 string
// - ServiceID: 4-byte length + UTF-8 string
// - ShapeID: 4-byte length + UTF-8 string
// - Direction: 1 byte (bool as uint8)
// - Headsign: 4-byte length + UTF-8 string
// - Stops: TripStopArray (see TripStopArray.Encode)
func (t Trip) Encode() []byte {
	routeIDStr := string(t.RouteID)
	serviceIDStr := string(t.ServiceID)
	shapeIDStr := string(t.ShapeID)
	headsignStr := t.Headsign

	stopsBytes := t.Stops.Encode()

	// Calculate total length
	totalLen := lenBytes + len(routeIDStr) + // RouteID
		lenBytes + len(serviceIDStr) + // ServiceID
		lenBytes + len(shapeIDStr) + // ShapeID
		boolBytes + // Direction
		lenBytes + len(headsignStr) + // Headsign
		len(stopsBytes) // Encoded Stops data

	data := make([]byte, totalLen)
	offset := 0

	// Marshal RouteID
	binary.BigEndian.PutUint32(data[offset:], uint32(len(routeIDStr)))
	offset += lenBytes
	copy(data[offset:], routeIDStr)
	offset += len(routeIDStr)

	// Marshal ServiceID
	binary.BigEndian.PutUint32(data[offset:], uint32(len(serviceIDStr)))
	offset += lenBytes
	copy(data[offset:], serviceIDStr)
	offset += len(serviceIDStr)

	// Marshal ShapeID
	binary.BigEndian.PutUint32(data[offset:], uint32(len(shapeIDStr)))
	offset += lenBytes
	copy(data[offset:], shapeIDStr)
	offset += len(shapeIDStr)

	// Marshal Direction
	if t.Direction {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	offset += boolBytes

	// Marshal Headsign
	binary.BigEndian.PutUint32(data[offset:], uint32(len(headsignStr)))
	offset += lenBytes
	copy(data[offset:], headsignStr)
	offset += len(headsignStr)

	// Append encoded Stops data
	copy(data[offset:], stopsBytes)
	// offset += len(stopsBytes) // Not strictly needed as it's the last part

	return data
}

// Decode the byte slice into the Trip struct
func (t *Trip) Decode(id Key, data []byte) error {
	if t == nil {
		return errors.New("cannot decode into a nil Trip")
	}
	offset := 0

	// Set ID from parameter
	t.ID = id

	// Unmarshal RouteID
	if offset+lenBytes > len(data) {
		return errors.New("trip buffer too small for RouteID length")
	}
	routeIDLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(routeIDLen) > len(data) {
		return errors.New("trip buffer too small for RouteID content")
	}
	t.RouteID = Key(data[offset : offset+int(routeIDLen)])
	offset += int(routeIDLen)

	// Unmarshal ServiceID
	if offset+lenBytes > len(data) {
		return errors.New("trip buffer too small for ServiceID length")
	}
	serviceIDLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(serviceIDLen) > len(data) {
		return errors.New("trip buffer too small for ServiceID content")
	}
	t.ServiceID = Key(data[offset : offset+int(serviceIDLen)])
	offset += int(serviceIDLen)

	// Unmarshal ShapeID
	if offset+lenBytes > len(data) {
		return errors.New("trip buffer too small for ShapeID length")
	}
	shapeIDLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(shapeIDLen) > len(data) {
		return errors.New("trip buffer too small for ShapeID content")
	}
	t.ShapeID = Key(data[offset : offset+int(shapeIDLen)])
	offset += int(shapeIDLen)

	// Unmarshal Direction
	if offset+boolBytes > len(data) {
		return errors.New("trip buffer too small for Direction")
	}
	if data[offset] == 1 {
		t.Direction = true
	} else if data[offset] == 0 {
		t.Direction = false
	} else {
		return fmt.Errorf("invalid byte value for bool (Direction): got %d, want 0 or 1", data[offset])
	}
	offset += boolBytes

	// Unmarshal Headsign
	if offset+lenBytes > len(data) {
		return errors.New("trip buffer too small for Headsign length")
	}
	headsignLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(headsignLen) > len(data) {
		return errors.New("trip buffer too small for Headsign content")
	}
	t.Headsign = string(data[offset : offset+int(headsignLen)])
	offset += int(headsignLen)

	// The rest of the data belongs to Stops
	if offset > len(data) {
		return errors.New("offset beyond data length before decoding Stops")
	}
	stopsData := data[offset:]
	err := t.Stops.Decode(stopsData)
	if err != nil {
		return fmt.Errorf("failed to decode Stops for Trip: %w", err)
	}
	// Trip.Decode has processed all parts. TripStopArray.Decode ensures its data is consumed.
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
			timepointInt = 0 // Default to 0 if conversion fails
		}
		// timepoint := TripTimepoint(timepointInt)
		var timepoint TripTimepoint
		if timepointInt == 0 {
			timepoint = ApproximateTripTimepoint
		} else {
			timepoint = ExactTripTimepoint
		}

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
		var direction TripDirection
		if directionInt == 0 {
			direction = OutboundTripDirection
		} else {
			direction = InboundTripDirection
		}
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
