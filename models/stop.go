package models

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/kelindar/column"
)

type LocationType uint8
type ModeFlag uint8

const (
	StopLocationType LocationType = iota
	StationLocationType
	EntranceExitLocationType
	GenericNodeLocationType
	BoardingAreaLocationType
	UnknownLocationType
)

const (
	BusModeFlag ModeFlag = 1 << iota
	SchoolBusModeFlag
	RailModeFlag
	FerryModeFlag
	UnknownModeFlag = 0
)

// Represents a stop, platform, or station in a transit system
type Stop struct {
	ID             Key
	Code           string
	Name           string
	ParentID       Key
	Location       Coordinate
	LocationType   LocationType
	SupportedModes ModeFlag
}
type StopMap map[Key]*Stop
type StopArray []*Stop

// Saves a stop to the database
func (s Stop) Save(r column.Row) error {
	r.SetString("code", s.Code)
	r.SetString("name", s.Name)
	r.SetString("parent_id", string(s.ParentID))
	r.SetString("location", s.Location.String())
	r.SetUint("location_type", uint(s.LocationType))
	r.SetUint("supported_modes", uint(s.SupportedModes))
	return nil
}

// Loads a stop from the database
func (s *Stop) Load(r column.Row) error {
	key, keyOk := r.Key()
	code, codeOk := r.String("code")
	name, nameOk := r.String("name")
	parentID, parentIDOk := r.String("parent_id")
	locationStr, locationStrOk := r.String("location")
	locationTypeInt, locationTypeIntOk := r.Uint("location_type")
	supportedModesInt, supportedModesIntOk := r.Uint("supported_modes")

	if !keyOk || !codeOk || !nameOk || !parentIDOk || !locationStrOk || !locationTypeIntOk || !supportedModesIntOk {
		return errors.New("missing required fields")
	}

	location, err := NewCoordinateFromString(locationStr)
	if err != nil {
		return err
	}

	*s = Stop{
		ID:             Key(key),
		Code:           code,
		Name:           name,
		ParentID:       Key(parentID),
		Location:       location,
		LocationType:   LocationType(locationTypeInt),
		SupportedModes: ModeFlag(supportedModesInt),
	}
	return nil
}

// Loads all stops from the database transaction
func (sa *StopArray) Load(txn *column.Txn) error {
	idCol := txn.Key()
	codeCol := txn.String("code")
	nameCol := txn.String("name")
	parentIDCol := txn.String("parent_id")
	locationCol := txn.String("location")
	locationTypeCol := txn.Uint("location_type")
	supportedModesCol := txn.Uint("supported_modes")

	count := txn.Count()
	if count == 0 {
		return nil
	}
	*sa = make(StopArray, count)

	var e error
	i := 0
	err := txn.Range(func(idx uint32) {
		id, idOk := idCol.Get()
		code, codeOk := codeCol.Get()
		name, nameOk := nameCol.Get()
		parentID, parentIDOk := parentIDCol.Get()
		locationStr, locationStrOk := locationCol.Get()
		locationTypeInt, locationTypeIntOk := locationTypeCol.Get()
		supportedModesInt, supportedModesIntOk := supportedModesCol.Get()

		if !idOk || !codeOk || !nameOk || !parentIDOk || !locationStrOk || !locationTypeIntOk || !supportedModesIntOk {
			e = errors.New("missing required fields")
			return
		}

		location, err := NewCoordinateFromString(locationStr)
		if err != nil {
			e = err
			return
		}

		(*sa)[i] = &Stop{
			ID:             Key(id),
			Code:           code,
			Name:           name,
			ParentID:       Key(parentID),
			Location:       location,
			LocationType:   LocationType(locationTypeInt),
			SupportedModes: ModeFlag(supportedModesInt),
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

// Parse a string into a ModeFlag
func parseModeFlag(mode string) ModeFlag {
	switch mode {
	case "Bus":
		return BusModeFlag
	case "School Bus":
		return SchoolBusModeFlag
	case "Rail":
		return RailModeFlag
	case "Ferry":
		return FerryModeFlag
	default:
		return UnknownModeFlag
	}
}

// Load and parse stops from the GTFS stops.txt file
func ParseStops(file io.Reader) (StopMap, error) {
	// Read file using CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	stops := make(StopMap)
	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into Stop struct
		id := Key(record[2])
		code := record[3]
		name := record[4]
		parentID := Key(record[1])

		lat, err := strconv.ParseFloat(record[6], 64)
		if err != nil {
			return nil, err
		}
		lon, err := strconv.ParseFloat(record[7], 64)
		if err != nil {
			return nil, err
		}
		location := Coordinate{
			Latitude:  lat,
			Longitude: lon,
		}

		typeInt, err := strconv.Atoi(record[0])
		if err != nil {
			typeInt = int(StopLocationType)
		}
		locationType := LocationType(typeInt)

		modes := ModeFlag(0)
		modeStrs := strings.SplitSeq(record[9], ",")
		for modeStr := range modeStrs {
			modes |= parseModeFlag(strings.TrimSpace(modeStr))
		}

		stops[id] = &Stop{
			ID:             id,
			Code:           code,
			Name:           name,
			ParentID:       parentID,
			Location:       location,
			LocationType:   locationType,
			SupportedModes: modes,
		}
	}

	return stops, nil
}
