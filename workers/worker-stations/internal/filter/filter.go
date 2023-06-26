// Package filter checks if the weather data is valid to go to the next stage
package filter

import (
	log "github.com/sirupsen/logrus"
	"strconv"
	"worker-station/internal/dtos"
)

// IsValid returns true if the following conditions are met:
// + The station code is equal or greater than 0
// + Name is not the empty string
func IsValid(stationData dtos.StationData) bool {
	validData := true
	var invalidReasons []string

	if stationData.Name == "" {
		invalidReasons = append(invalidReasons, "No name for station")
		validData = false
	}

	stationCodeInt, _ := strconv.Atoi(stationData.Code)

	if stationCodeInt < 0 {
		invalidReasons = append(invalidReasons, "Station code < 0")
		validData = false
	}

	if !validData {
		log.Infof("[Stations Worker] Invalid data, reasons: %v", invalidReasons)
	}

	return validData
}
