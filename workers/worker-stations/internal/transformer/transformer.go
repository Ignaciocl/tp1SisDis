package transformer

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"worker-station/internal/dtos"
	stationErrors "worker-station/internal/errors"
)

const (
	dataFieldDelimiter = ","
	codeIdx            = 5
	nameIdx            = 6
	latitudeIdx        = 7
	longitudeIdx       = 8
	yearIdx            = 9
)

// Transform transforms raw station data into Station Data
func Transform(rawStationData string) (*dtos.StationData, error) {
	dataSplit := strings.Split(rawStationData, dataFieldDelimiter)

	stationCode, err := strconv.Atoi(dataSplit[codeIdx])
	if err != nil {
		log.Debugf("Invalid station code ID: %v", dataSplit[codeIdx])
		return nil, fmt.Errorf("%w: %s", stationErrors.ErrInvalidStationData, stationErrors.ErrStationCodeType)
	}

	latitude := dataSplit[latitudeIdx] // for query 2 we don't care about these values, so we cannot filter them. We add a fake value to avoid errors
	longitude := dataSplit[longitudeIdx]

	yearID, err := strconv.Atoi(dataSplit[yearIdx])
	if err != nil {
		log.Debugf("Invalid year ID: %v", dataSplit[yearIdx])
		return nil, fmt.Errorf("%w: %s", stationErrors.ErrInvalidStationData, stationErrors.ErrInvalidYearIDType)
	}

	return &dtos.StationData{
		Code:      fmt.Sprintf("%v", stationCode),
		Name:      dataSplit[nameIdx],
		Latitude:  latitude,
		Longitude: longitude,
		Year:      yearID,
	}, nil
}
