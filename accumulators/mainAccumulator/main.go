package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
)

const serviceName = "accumulator-main"

type Accumulator struct {
	Stations    []string `json:"stations,omitempty"`
	AvgStations []string `json:"avg_stations,omitempty"`
	Duration    *float64 `json:"duration"`
	ClientID    string   `json:"client_Id"`
	common.EofData
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
	Data QueryResponse `json:"query_result"`
}

type actionable struct {
	nc  chan string
	msg string
}

func (a actionable) DoActionIfEOF() {
	a.nc <- a.msg // continue the loop
}

func main() {
	inputQueue, _ := queue.InitializeReceiver[Accumulator]("accumulator", "rabbit", "", "", nil)
	me, _ := common.CreateConsumerEOF(nil, "accumulator", inputQueue, 3)
	accumulatorInfo, _ := queue.InitializeSender[QueryResult]("accConnection", 0, nil, "rabbit")
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer accumulatorInfo.Close()
	defer inputQueue.Close()
	ns := make(chan string, 1)
	acc := make(map[string]QueryResponse)
	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if data.EOF {

				me.AnswerEofOk(data.IdempotencyKey, actionable{nc: ns, msg: data.IdempotencyKey})
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				log.Infof("message for idempotency received: %+v", data)
				continue
			}
			if err != nil {
				utils.LogError(err, "Failed while receiving message")
				inputQueue.RejectMessage(msgId)
				continue
			}
			processData(data, acc)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()
	go func() {
		key := <-ns
		qr := QueryResult{Data: acc[key]}

		accumulatorInfo.SendMessage(qr, "")
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "health check error")
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}
