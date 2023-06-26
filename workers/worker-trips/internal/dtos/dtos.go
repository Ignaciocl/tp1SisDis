package dtos

import "github.com/Ignaciocl/tp1SisdisCommons/dtos"

type TripData struct {
	StartStationCode string  `json:"start_station_code"`
	StartDate        string  `json:"start_date"`
	EndStationCode   string  `json:"end_station_code"`
	Duration         float64 `json:"duration_sec,string"`
	Year             int     `json:"year"`
}

type InputData struct {
	Metadata dtos.Metadata `json:"metadata"`
	Data     []string      `json:"data"`
}

type SendableDataMontreal struct {
	StartStationCode string `json:"o_station"`
	EndStationCode   string `json:"e_station"`
	Year             int    `json:"year"`
}

type SendableDataDuplicates struct {
	StationCode string `json:"station"`
	Year        int    `json:"year"`
}

type SendableDataRainfall struct {
	Duration float64 `json:"duration"`
	Date     string  `json:"date"`
}

type JoinerData[T any] struct {
	IdempotencyKey string `json:"key"`
	City           string `json:"city"`
	Data           []T    `json:"tripData"`
	EOF            bool   `json:"EOF"`
}

func NewSendableDataMontrealFromTrip(trip TripData) SendableDataMontreal {
	return SendableDataMontreal{
		StartStationCode: trip.StartStationCode,
		EndStationCode:   trip.EndStationCode,
		Year:             trip.Year,
	}
}

func NewSendableDataRainfallFromTrip(trip TripData) SendableDataRainfall {
	return SendableDataRainfall{
		Duration: trip.Duration,
		Date:     trip.StartDate,
	}
}

func NewSendableDataDuplicatesFromTrip(trip TripData) SendableDataDuplicates {
	return SendableDataDuplicates{
		StationCode: trip.StartStationCode,
		Year:        trip.Year,
	}
}
