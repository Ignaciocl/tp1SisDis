package main

import (
	"encoding/json"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
)

type ReceivableDataWeather struct {
	Date string `json:"date"`
}

type ReceivableDataTrip struct {
	Date     string `json:"date"`
	Duration int    `json:"duration"`
}

type JoinerDataStation struct {
	DataWeather *ReceivableDataWeather `json:"weatherData,omitempty"`
	DataTrip    *[]ReceivableDataTrip  `json:"tripData,omitempty"`
	Name        string                 `json:"name"`
	ClientID    string                 `json:"client_id"`
	City        string                 `json:"city"`
	common.EofData
}

func (j JoinerDataStation) GetId() int64 {
	return 0
}

func (j JoinerDataStation) SetId(id int64) {
}

type weatherDuration struct {
	total    int
	duration int
}

type WeatherDuration struct {
	Total    int `json:"total"`
	Duration int `json:"duration"`
}

type ToAccWeather struct {
	Data WeatherDuration `json:"data"`
	common.EofData
	ClientID string `json:"client_id"`
}

type transformer struct {
}

func (t transformer) ToWritable(data JoinerDataStation) []string {
	a, _ := json.Marshal(data)
	return []string{string(a)}
}

func (t transformer) FromWritable(d []string) JoinerDataStation {
	var r JoinerDataStation
	utils.LogError(json.Unmarshal([]byte(d[0]), &r), "failed to unmarshall while reading")
	return r
}
