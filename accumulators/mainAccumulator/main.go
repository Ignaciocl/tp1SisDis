package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Accumulator struct {
	Stations    []string `json:"stations,omitempty"`
	AvgStations []string `json:"avg_stations,omitempty"`
	Duration    *float64 `json:"duration"`
	Key         string   `json:"key"`
}

type QueryResponse struct {
	Montreal  []string `json:"montreal"`
	Avg       []string `json:"avgStations"`
	AvgMore30 *float64 `json:"avgMore30"`
}

func processData(data Accumulator, acc map[string]QueryResponse) {
	if data.Stations != nil {
		d := getQueryResponse(data.Key, acc)
		d.Montreal = data.Stations
		acc["random"] = d
	} else if data.AvgStations != nil { // Add here the logic of inter values (not for all cities but for specifics), could be done after connecting everything
		d := getQueryResponse(data.Key, acc)
		d.Avg = data.AvgStations
		acc["random"] = d
	} else if data.Duration != nil {
		d := getQueryResponse(data.Key, acc)
		d.Avg = data.AvgStations
		acc["random"] = d
	}
}

func getQueryResponse(key string, acc map[string]QueryResponse) QueryResponse {
	d, ok := acc[key]
	if !ok {
		d = QueryResponse{Montreal: nil}
	}
	return d
}

type checker struct {
	blocker     chan struct{}
	eofReceived chan struct{}
}

type QueryResult struct {
	Data QueryResponse `json:"query_result"`
}

func (c checker) IsStillUsingNecessaryDataForFile(file string, city string) bool {
	d := <-c.blocker
	log.Printf("EOF received for file %s and city %s\n", file, city)
	c.eofReceived <- d
	c.blocker <- d
	return true
}

func main() {
	inputQueue, _ := common.InitializeRabbitQueue[Accumulator, Accumulator]("accumulator", "rabbit")
	wfe, _ := common.CreateConsumerEOF("rabbit", "accumulatorEOF")
	accumulatorInfo, _ := common.InitializeRabbitQueue[QueryResult, QueryResult]("accConnection", "rabbit")
	blocker := make(chan struct{}, 1)
	eofReceived := make(chan struct{}, 1)
	blocker <- struct{}{}
	c := checker{blocker: blocker, eofReceived: eofReceived}
	go func() {
		wfe.AnswerEofOk(c)
	}()
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	acc := make(map[string]QueryResponse)
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			d := <-blocker
			log.Printf("data is: %v\n", data)
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
			blocker <- d
		}
	}()
	go func() {
		r := 0
		for {
			<-eofReceived
			r += 1
			if r > 8 {
				break
			}
		}
		for {
			d := <-blocker
			if inputQueue.IsEmpty() {
				break
			}
			blocker <- d
		}
		qr := QueryResult{Data: acc["random"]}
		log.Printf("ALL EOF RECEIVEDL %v\n", qr)
		accumulatorInfo.SendMessage(qr)
		r = 0
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
