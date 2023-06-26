package errors

import "errors"

var (
	ErrInvalidWeatherData  = errors.New("error invalid weather data")
	ErrInvalidDate         = errors.New("invalid date")
	ErrInvalidRainfallType = errors.New("invalid rainfall type")
)
