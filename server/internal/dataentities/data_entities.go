package dataentities

import log "github.com/sirupsen/logrus"

type FileData struct {
	EOF  *bool         `json:"eof"`
	File string        `json:"file"`
	Data []interface{} `json:"data"`
}

type DataToSend struct {
	File           string        `json:"file,omitempty"`
	Data           []interface{} `json:"data,omitempty"`
	City           string        `json:"city,omitempty"`
	IdempotencyKey string        `json:"idempotencyKey"`
}

type AccumulatorData struct {
	QueryResult map[string]interface{} `json:"query_result"`
}

type DataQuery struct {
	Sem  chan struct{}
	Data map[string]map[string]interface{}
}

func (dq DataQuery) GetQueryValue(key string) (map[string]interface{}, bool) {
	log.Infof("start checking query value")
	d := <-dq.Sem
	value, ok := dq.Data[key]
	dq.Sem <- d
	log.Infof("finish checking query value")
	return value, ok
}

func (dq DataQuery) WriteQueryValue(data map[string]interface{}, userID string) {
	d := <-dq.Sem
	dq.Data[userID] = data
	dq.Sem <- d
}
