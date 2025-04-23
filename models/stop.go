package models

import (
	"database/sql"
	"encoding/csv"
	"io"
	"strconv"
	"strings"
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

// Encode the Stop struct into a record
func (s *Stop) Encode() []any {
	return []any{
		string(s.ID),
		string(s.ParentID),
		s.Code,
		s.Name,
		s.Location.String(),
		int(s.LocationType),
		int(s.SupportedModes),
	}
}

// Decode a record into a Stop struct
func DecodeStop(record *sql.Row) (*Stop, error) {
	// if len(record) < 7 {
	// 	return nil, errors.New("record does not contain enough fields")
	// }

	// id := Key(record[0].(string))
	// parentID := Key(record[1].(string))
	// code := record[2].(string)
	// name := record[3].(string)

	// locationStr := record[4].(string)
	// location, err := NewCoordinateFromString(locationStr)
	// if err != nil {
	// 	return nil, err
	// }

	// locationTypeInt, err := strconv.Atoi(record[5].(string))
	// if err != nil {
	// 	return nil, err
	// }
	// locationType := LocationType(locationTypeInt)

	// supportedModesInt, err := strconv.Atoi(record[6].(string))
	// if err != nil {
	// 	return nil, err
	// }
	// supportedModes := ModeFlag(supportedModesInt)

	var id, code, name, parentID, locationStr string
	var locationTypeInt, supportedModesInt int
	err := record.Scan(&id, &parentID, &code, &name, &locationStr, &locationTypeInt, &supportedModesInt)
	if err != nil {
		return nil, err
	}

	location, err := NewCoordinateFromString(locationStr)
	if err != nil {
		return nil, err
	}

	return &Stop{
		ID:             Key(id),
		Code:           code,
		Name:           name,
		ParentID:       Key(parentID),
		Location:       location,
		LocationType:   LocationType(locationTypeInt),
		SupportedModes: ModeFlag(supportedModesInt),
	}, nil
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
