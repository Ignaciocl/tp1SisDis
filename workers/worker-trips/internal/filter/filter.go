// Package filter checks if the weather data is valid to go to the next stage
package filter

import (
	log "github.com/sirupsen/logrus"
	"strconv"
	"worker-trips/internal/dtos"
)

// ValidStationCodes returns true if both station codes are integer equal or greater than zero
func ValidStationCodes(tripData dtos.TripData) bool {
	validData := true
	var invalidReasons []string

	startStationCodeInt, _ := strconv.Atoi(tripData.StartStationCode)
	endStationCodeInt, _ := strconv.Atoi(tripData.EndStationCode)

	if startStationCodeInt < 0 {
		validData = false
		invalidReasons = append(invalidReasons, "Start Station code < 0")
	}

	if endStationCodeInt < 0 {
		validData = false
		invalidReasons = append(invalidReasons, "End Station code < 0")
	}

	if !validData {
		log.Infof("[Stations Worker] Invalid data, reasons: %v", invalidReasons)
	}

	return validData
}

// ValidDuration returns true if the duration is greater than 0
func ValidDuration(tripData dtos.TripData) bool {
	return tripData.Duration >= 0.0
}
