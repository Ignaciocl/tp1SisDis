package transformer

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
	"worker-trips/internal/dtos"
	tripErrors "worker-trips/internal/errors"
)

const (
	dataFieldDelimiter  = ","
	dateLayout          = "2006-01-02"
	startDateIdx        = 5
	startStationCodeIdx = 6
	endDateIdx          = 7
	endStationCodeIdx   = 8
	durationIdx         = 9
	yearIdx             = 11
)

// Transform transforms raw station data into TripData
func Transform(rawTripData string) (*dtos.TripData, error) {
	dataSplit := strings.Split(rawTripData, dataFieldDelimiter)

	startDateStr := dataSplit[startDateIdx]
	startDateStr = strings.Split(startDateStr, " ")[0] // To avoid hours:minutes:seconds
	_, err := time.Parse(dateLayout, startDateStr)
	/*if err != nil {
		log.Debugf("Invalid start date: %v", dataSplit[startDateIdx])
		return nil, fmt.Errorf("%w: %s", tripErrors.ErrInvalidTripData, tripErrors.ErrInvalidDate)
	}*/

	endDateStr := dataSplit[endDateIdx]
	endDateStr = strings.Split(endDateStr, " ")[0]
	_, err = time.Parse(dateLayout, endDateStr)
	/*if err != nil {
		log.Debugf("Invalid end date; %v", dataSplit[endDateIdx])
		return nil, fmt.Errorf("%w: %s", tripErrors.ErrInvalidTripData, tripErrors.ErrInvalidDate)
	}*/

	startStationCodeID, err := strconv.Atoi(dataSplit[startStationCodeIdx])
	/*if err != nil {
		log.Debugf("Invalid start station code ID: %v", dataSplit[startStationCodeID])
		return nil, fmt.Errorf("%w: %s", tripErrors.ErrInvalidTripData, tripErrors.ErrStationCodeType)
	}*/

	endStationCodeID, err := strconv.Atoi(dataSplit[endStationCodeIdx])
	/*if err != nil {
		log.Debugf("Invalid end station code ID: %v", dataSplit[endStationCodeIdx])
		return nil, fmt.Errorf("%w: %s", tripErrors.ErrInvalidTripData, tripErrors.ErrStationCodeType)
	}*/

	yearID, err := strconv.Atoi(dataSplit[yearIdx])
	/*if err != nil {
		log.Debugf("Invalid year ID: %v", dataSplit[yearIdx])
		return nil, fmt.Errorf("%w: %s", tripErrors.ErrInvalidTripData, tripErrors.ErrInvalidYearIDType)
	}*/

	duration, err := strconv.ParseFloat(dataSplit[durationIdx], 64)
	if err != nil {
		log.Debugf("Invalid duration type: %v", dataSplit[durationIdx])
		return nil, fmt.Errorf("%w: %s", tripErrors.ErrInvalidTripData, tripErrors.ErrInvalidDurationType)
	}
	if duration < 0.0 {
		duration = 0.0
	}

	return &dtos.TripData{
		StartStationCode: fmt.Sprintf("%v", startStationCodeID),
		StartDate:        startDateStr,
		EndStationCode:   fmt.Sprintf("%v", endStationCodeID),
		Duration:         duration,
		Year:             yearID,
	}, nil
}
