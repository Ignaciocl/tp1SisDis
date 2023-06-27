package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"strconv"
	"strings"
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
	if !data.EOF {
		s := data.DataStation
		sData := []string{s.Name, s.Code, s.Name, strconv.Itoa(s.Year)}
		return []string{data.ClientID, data.IdempotencyKey, strconv.FormatBool(data.EOF), strings.Join(sData, Sep)}
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
		s := strings.Split(d[3], Sep)
		year, _ := strconv.Atoi(s[3])
		r.DataStation = &ReceivableDataStation{
			Code: s[1],
			Name: s[0],
			Year: year,
		}
	}
	return r
}
