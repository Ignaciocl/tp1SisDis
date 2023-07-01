package main

import (
	"encoding/json"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	"strings"
)

const eofStorageFilename = "eof.csv"

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
