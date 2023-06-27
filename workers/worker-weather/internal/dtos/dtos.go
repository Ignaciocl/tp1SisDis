package dtos

import (
	commons "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/dtos"
)

type InputData struct {
	Metadata dtos.Metadata `json:"metadata"`
	Data     []string      `json:"data"`
}
type WeatherData struct {
	Date string  `json:"date"`
	Prec float64 `json:"prec"`
}

type JoinerData struct {
	City     string      `json:"city"`
	Data     WeatherData `json:"weatherData"`
	ClientID string      `json:"client_id"`
	commons.EofData
}
