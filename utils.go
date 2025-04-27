package gtfs

import "time"

// Removes the timezone from a time.Time object and returns it in UTC
func RemoveTimezone(t time.Time) time.Time {
	return time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		0, time.UTC)
}
