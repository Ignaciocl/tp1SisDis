package main

import (
	"encoding/json"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
)

const Sep = "|PONG|"

type ReceivableDataStation struct {
	Code string `json:"code"`
	Name string `json:"name"`
	Year int    `json:"year"`
}

type ReceivableDataTrip struct {
	Station string `json:"station"`
	Year    int    `json:"year"`
}

type JoinerDataStation struct {
	DataStation *ReceivableDataStation `json:"stationData,omitempty"`
	DataTrip    *[]ReceivableDataTrip  `json:"tripData,omitempty"`
	Name        string                 `json:"name"`
	ClientID    string                 `json:"client_id"`
	City        string                 `json:"city"`
	common.EofData
}

type senderDataStation struct {
	Name string `json:"name"`
	Year int    `json:"year"`
}

type PreAccumulatorData struct {
	Data     []senderDataStation `json:"data"`
	ClientID string              `json:"client_id"`
	common.EofData
}

type stationData struct {
	name string
}

type stationAlive struct {
	wasAliveOn17 bool
	wasAliveOn16 bool
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
