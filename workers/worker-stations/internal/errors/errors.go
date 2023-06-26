package errors

import "errors"

var (
	ErrInvalidStationData = errors.New("error invalid station data")
	ErrStationCodeType    = errors.New("invalid station code type")
	ErrInvalidYearIDType  = errors.New("invalid year ID type")
)
