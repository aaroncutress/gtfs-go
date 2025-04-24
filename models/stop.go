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

// Saves the stop to the database
func (s Stop) Save(r column.Row) error {
	r.SetString("code", s.Code)
	r.SetString("name", s.Name)
	r.SetString("parent_id", string(s.ParentID))
	r.SetString("location", s.Location.String())
	r.SetUint("location_type", uint(s.LocationType))
	r.SetUint("supported_modes", uint(s.SupportedModes))
	return nil
}

// Loads the stop from the database
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

	s.ID = Key(key)
	s.Code = code
	s.Name = name
	s.ParentID = Key(parentID)
	s.Location = location
	s.LocationType = LocationType(locationTypeInt)
	s.SupportedModes = ModeFlag(supportedModesInt)

	return nil
}

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

// Load stops from the GTFS stops.txt file
func LoadStops(file io.Reader) (StopMap, error) {
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
