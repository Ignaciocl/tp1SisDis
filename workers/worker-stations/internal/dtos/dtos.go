package dtos

import (
	"github.com/Ignaciocl/tp1SisdisCommons/dtos"
)

type InputData struct {
	Metadata dtos.Metadata `json:"metadata"`
	Data     []string      `json:"data"`
}
type JoinerDataStation struct {
	City           string      `json:"city"`
	Data           StationData `json:"stationData"`
	IdempotencyKey string      `json:"key"`
	EOF            bool        `json:"EOF"`
}

type StationData struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
	Year      int    `json:"year"`
}
