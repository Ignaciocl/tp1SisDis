package main

import (
	"encoding/json"
	"errors"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/fileManager"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"io"
	"strings"
)

const (
	serviceName = "accumulator-main"
	Sep         = "-ENDLINE-"
)

type Accumulator struct {
	Stations    []string `json:"stations,omitempty"`
	AvgStations []string `json:"avg_stations,omitempty"`
	Duration    *float64 `json:"duration"`
	ClientID    string   `json:"client_Id"`
	common.EofData
}

func (a Accumulator) GetId() int64 {
	return 0
}

func (a Accumulator) SetId(id int64) {
}

type QueryResponse struct {
	Montreal  []string `json:"montreal"`
	Avg       []string `json:"avgStations"`
	AvgMore30 *float64 `json:"avgMore30"`
}

func processData(data Accumulator, acc map[string]QueryResponse) {
	if data.Stations != nil {
		d := getQueryResponse(data.ClientID, acc)
		d.Montreal = data.Stations
		acc[data.ClientID] = d
		log.Infof("received response query from montreal (query3): %v", d)
	} else if data.AvgStations != nil {
		d := getQueryResponse(data.ClientID, acc)
		d.Avg = data.AvgStations
		acc[data.ClientID] = d
		log.Infof("received response query from stations (query2): %v", d)
	} else if data.Duration != nil {
		d := getQueryResponse(data.ClientID, acc)
		d.AvgMore30 = data.Duration
		acc[data.ClientID] = d
		log.Infof("received response query from weather (query1): %v", d)
	}
}

func getQueryResponse(key string, acc map[string]QueryResponse) QueryResponse {
	d, ok := acc[key]
	if !ok {
		d = QueryResponse{Montreal: nil}
	}
	return d
}

type QueryResult struct {
	Data     QueryResponse `json:"query_result"`
	ClientId string        `json:"client_id"`
}

type actionable struct {
	nc  chan string
	msg string
}

func (a actionable) DoActionIfEOF() {
	a.nc <- a.msg // continue the loop
}

type t struct{}

func (t t) ToWritable(data *Accumulator) []byte {
	d, _ := json.Marshal(data)
	return d
}

func (t t) FromWritable(d []byte) *Accumulator {
	data := strings.Split(string(d), Sep)[0]
	var r Accumulator
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		utils.LogError(err, "could not unmarshal from db")
	}
	return &r
}

func main() {
	inputQueue, _ := queue.InitializeReceiver[Accumulator]("accumulator", "rabbit", "", "", nil)
	me, _ := common.CreateConsumerEOF(nil, "accumulator", inputQueue, 3)
	accumulatorInfo, _ := queue.InitializeSender[QueryResult]("accConnection", 0, nil, "rabbit")
	grace, _ := common.CreateGracefulManager("rabbit")
	acc := make(map[string]QueryResponse)
	ns := make(chan string, 1)
	db, err := fileManager.CreateDB[*Accumulator](t{}, "accumulator_store.csv", 100000, Sep) // reinvent a new db just for the accumulator, this is a hotfix
	utils.FailOnError(err, "could not create db")
	utils.FailOnError(fillData(acc, ns, db, me), "could not read from db")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer accumulatorInfo.Close()
	defer inputQueue.Close()
	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if err != nil {
				utils.LogError(err, "Failed while receiving message")
				inputQueue.RejectMessage(msgId)
				continue
			}
			utils.LogError(db.Write(&data), "could not update row on db")
			if data.EOF {

				me.AnswerEofOk(data.IdempotencyKey, actionable{nc: ns, msg: data.IdempotencyKey})
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				log.Infof("message for idempotency received: %+v", data)
				continue
			}
			processData(data, acc)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()
	go func() {
		key := <-ns
		qr := QueryResult{Data: acc[key], ClientId: key}
		log.Infof("sending to server data: %+v", qr)

		utils.FailOnError(accumulatorInfo.SendMessage(qr, ""), "could not report to server, panicking")
		delete(acc, key)
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "health check error")
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

func fillData(acc map[string]QueryResponse, ns chan string, db fileManager.Manager[*Accumulator], me common.WaitForEof) error {
	m := make(map[string]int)
	for {
		line, err := db.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if line.EOF {
			i := m[line.IdempotencyKey]
			i += 1
			m[line.IdempotencyKey] = i
			continue
		}
		processData(*line, acc)
	}
	for k, v := range m {
		if v%3 == 0 {
			for i := 0; i < 3; i += 1 {
				me.AnswerEofOk(k, actionable{nc: ns, msg: k})
			}
		} else {
			for i := 0; i < v%3; i += 1 {
				me.AnswerEofOk(k, actionable{nc: ns, msg: k})
			}
		}
	}
	return nil
}
