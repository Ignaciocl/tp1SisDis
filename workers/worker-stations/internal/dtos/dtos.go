package dtos

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/dtos"
)

type InputData struct {
	Metadata dtos.Metadata `json:"metadata"`
	Data     []string      `json:"data"`
}
type JoinerDataStation struct {
	City     string      `json:"city"`
	Data     StationData `json:"stationData"`
	ClientID string      `json:"client_id"`
	common.EofData
}

type StationData struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
	Year      int    `json:"year"`
}
