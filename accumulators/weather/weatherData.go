package main

import (
	"encoding/json"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	"strings"
)

const Sep = "-impos-"

type WeatherDuration struct {
	Total              int    `json:"total"`
	Duration           int    `json:"duration"`
	Id                 int64  `json:"id"`
	LastIdempotencyKey string `json:"last_idempotency_key"`
}
type Receivable struct {
	Data WeatherDuration `json:"data"`
	common.EofData
	ClientID string `json:"client_id"`
}

func (r *WeatherDuration) GetId() int64 {
	return r.Id
}

func (r *WeatherDuration) SetId(id int64) {
	r.Id = id
}

type AccumulatorData struct {
	Duration float64 `json:"duration"`
	ClientID string  `json:"client_id"`
	common.EofData
}

type t struct {
}

func (t t) ToWritable(data *WeatherDuration) []byte {
	d, _ := json.Marshal(data)
	return d
}

func (t t) FromWritable(d []byte) *WeatherDuration {
	data := strings.Split(string(d), Sep)[0]
	var r WeatherDuration
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
