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
	Key         string                 `json:"key"`
	common.EofData
}

type AccumulatorData struct {
	Dur float64 `json:"duration"`
	Key string  `json:"key"`
	common.EofData
}

type preAccumulatorData struct {
	DurGathered int
	Amount      int
}

type weatherDuration struct {
	total    int
	duration int
}

type transformer struct {
}

func (t transformer) ToWritable(data JoinerDataStation) []string {
	if !data.EOF {
		s := data.DataWeather
		return []string{data.Key, data.IdempotencyKey, strconv.FormatBool(data.EOF), s.Date}
	}
	return []string{data.Key, data.IdempotencyKey, strconv.FormatBool(data.EOF), ""}
}

func (t transformer) FromWritable(d []string) JoinerDataStation {
	eof, _ := strconv.ParseBool(d[2])
	r := JoinerDataStation{
		Key: d[0],
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
