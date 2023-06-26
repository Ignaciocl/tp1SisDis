package dtos

import (
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
	City           string      `json:"city"`
	Data           WeatherData `json:"weatherData"`
	IdempotencyKey string      `json:"key"`
	EOF            bool        `json:"EOF"`
}
