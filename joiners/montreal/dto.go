package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"strconv"
	"strings"
)

const Sep = "/-my pretty separator-/"

type SendableDataStation struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
	Year      int    `json:"year"`
}

type SendableDataTrip struct {
	OStation string `json:"o_station"`
	EStation string `json:"e_station"`
	Year     int    `json:"year"`
}
type JoinerDataStation struct {
	DataStation *SendableDataStation `json:"stationData,omitempty"`
	DataTrip    *[]SendableDataTrip  `json:"tripData,omitempty"`
	Name        string               `json:"name,omitempty"`
	Key         string               `json:"key,omitempty"`
	common.EofData
}

type sData struct {
	Lat  string
	Long string
	Name string
}

type AccumulatorData struct {
	OLat  string `json:"o_lat"`
	OLong string `json:"o_long"`
	FLat  string `json:"f_lat"`
	FLong string `json:"f_long"`
	Name  string `json:"name"`
}

type transformer struct {
}

func (t transformer) ToWritable(data JoinerDataStation) []string {
	s := data.DataStation
	if data.EOF {
		return []string{data.Key, data.IdempotencyKey, strconv.FormatBool(data.EOF), ""}
	}
	stationData := []string{s.Name, s.Code, s.Longitude, s.Latitude, strconv.Itoa(s.Year)}
	return []string{data.Key, data.IdempotencyKey, strconv.FormatBool(data.EOF), strings.Join(stationData, Sep)}
}

func (t transformer) FromWritable(d []string) JoinerDataStation {
	eof, _ := strconv.ParseBool(d[2])
	r := JoinerDataStation{
		Key: d[0],
	}
	r.EOF = eof
	r.IdempotencyKey = d[1]
	if !eof {
		s := strings.Split(d[3], Sep)
		year, _ := strconv.Atoi(s[4])
		r.DataStation = &SendableDataStation{
			Code:      s[1],
			Name:      s[0],
			Latitude:  s[3],
			Longitude: s[2],
			Year:      year,
		}
	}
	return r
}
