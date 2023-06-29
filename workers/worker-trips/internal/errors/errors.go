package errors

import "errors"

var (
	ErrInvalidTripData     = errors.New("error invalid trip data")
	ErrStationCodeType     = errors.New("invalid station code type")
	ErrInvalidYearIDType   = errors.New("invalid year ID type")
	ErrInvalidDate         = errors.New("invalid date")
	ErrInvalidDurationType = errors.New("invalid duration type")
)
