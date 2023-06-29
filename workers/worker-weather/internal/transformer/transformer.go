// Package transformer transforms the input data into a Weather struct
package transformer

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
	"worker-weather/internal/dtos"
	weatherErrors "worker-weather/internal/errors"
)

const (
	dataFieldDelimiter = ","
	dateLayout         = "2006-01-02"
	dateIdx            = 5
	rainfallIdx        = 6
)

func Transform(rawWeatherData string) (*dtos.WeatherData, error) {
	dataSplit := strings.Split(rawWeatherData, dataFieldDelimiter)
	date, err := time.Parse(dateLayout, dataSplit[dateIdx])
	if err != nil {
		log.Debugf("Invalid date %s", dataSplit[dateIdx])
		return nil, fmt.Errorf("%w: %s", weatherErrors.ErrInvalidWeatherData, weatherErrors.ErrInvalidDate)
	}

	// The data of the rainfall is for the previous data
	date = date.AddDate(0, 0, -1)

	rainfall, err := strconv.ParseFloat(dataSplit[rainfallIdx], 64)
	if err != nil {
		log.Debugf("Invalid rainfall type")
		return nil, fmt.Errorf("%w: %s", weatherErrors.ErrInvalidWeatherData, weatherErrors.ErrInvalidRainfallType)
	}

	// We dont want hours, minutes, etc
	dateAsString := strings.Split(date.String(), " ")[0]

	return &dtos.WeatherData{
		Date: dateAsString,
		Prec: rainfall,
	}, nil
}
