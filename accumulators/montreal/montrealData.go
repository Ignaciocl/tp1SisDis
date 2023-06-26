package main

import (
	"encoding/json"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	"strings"
)

const Sep = "-DATAENDED-"

type dStation struct {
	Counter            float64 `json:"counter"`
	DistanceCounted    float64 `json:"distanceCounted"`
	LastIdempotencyKey string  `json:"last_idempotency_key"`
	Id                 int64   `json:"id"`
}

func (d *dStation) add(distance float64) {
	d.DistanceCounted += distance
	d.Counter += 1
}

func (d *dStation) didItWentMoreThan(distanceAvg float64) bool {
	return (d.DistanceCounted / d.Counter) > distanceAvg
}

func (d *dStation) GetId() int64 {
	return d.Id
}

func (d *dStation) SetId(id int64) {
	d.Id = id
}

type t struct{}

func (t t) ToWritable(data *dStation) []byte {
	returnable, _ := json.Marshal(data)
	return returnable
}

func (t t) FromWritable(d []byte) *dStation {
	data := strings.Split(string(d), Sep)[0]
	var r dStation
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
