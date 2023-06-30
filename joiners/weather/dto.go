package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"strconv"
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
	if !data.EOF {
		s := data.DataWeather
		return []string{data.ClientID, data.IdempotencyKey, strconv.FormatBool(data.EOF), s.Date}
	}
	return []string{data.ClientID, data.IdempotencyKey, strconv.FormatBool(data.EOF), ""}
}

func (t transformer) FromWritable(d []string) JoinerDataStation {
	eof, _ := strconv.ParseBool(d[2])
	r := JoinerDataStation{
		ClientID: d[0],
	}
	r.EOF = eof
	r.IdempotencyKey = d[1]
	if !eof {
		s := d[3]
		r.DataWeather = &ReceivableDataWeather{
			Date: s,
		}
	}
	return r
}
