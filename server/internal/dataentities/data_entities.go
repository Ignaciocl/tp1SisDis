package dataentities

import (
	"github.com/Ignaciocl/tp1SisdisCommons/dtos"
	log "github.com/sirupsen/logrus"
)

type DataToSend struct {
	Metadata dtos.Metadata `json:"metadata"`
	Data     []string      `json:"data,omitempty"`
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
