package main

import (
	"encoding/json"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	"strings"
)

const Sep = "-DATAENDED-"

type stationData struct {
	SweetSixteen          int    `json:"sweet_sixteen"`
	SadSeventeen          int    `json:"sad_seventeen"`
	Name                  string `json:"name"`
	Id                    int64  `json:"id"`
	LastSetIdempotencyKey string `json:"last_set_idempotency_key"`
}

func (sd *stationData) wasDouble() bool {
	return sd.SadSeventeen > 2*sd.SweetSixteen
}

func (sd *stationData) addYear(year int) {
	if year == 2016 {
		sd.SweetSixteen += 1
	} else if year == 2017 {
		sd.SadSeventeen += 1
	}
}

func (sd *stationData) GetId() int64 {
	return sd.Id
}

func (sd *stationData) SetId(id int64) {
	sd.Id = id
}

type t struct{}

func (t t) ToWritable(data *stationData) []byte {
	returnable, _ := json.Marshal(data)
	return returnable
}

func (t t) FromWritable(d []byte) *stationData {
	data := strings.Split(string(d), Sep)[0]
	var r stationData
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		utils.LogError(err, "could not unmarshal from db")
	}
	return &r
}

type eofData struct {
	IdempotencyKey string `json:"idempotency_key"`
	Id             int64  `json:"id"`
}

func (e *eofData) GetId() int64 {
	return e.Id
}

func (e *eofData) SetId(id int64) {
	e.Id = id
}

type t2 struct{}

func (t t2) ToWritable(data *eofData) []byte {
	returnable, _ := json.Marshal(data)
	return returnable
}

func (t t2) FromWritable(d []byte) *eofData {
	data := strings.Split(string(d), Sep)[0]
	var r eofData
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		utils.LogError(err, "could not unmarshal from db")
	}
	return &r
}
