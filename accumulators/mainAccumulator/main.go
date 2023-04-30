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
	AvgStations []string `json:"avgStations,omitempty"`
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
		acc[data.Key] = d
	} else if data.AvgStations != nil { // Add here the logic of inter values (not for all cities but for specifics), could be done after connecting everything
		d := getQueryResponse(data.Key, acc)
		d.Avg = data.AvgStations
		acc[data.Key] = d
	} else if data.Duration != nil {
		d := getQueryResponse(data.Key, acc)
		d.Avg = data.AvgStations
		acc[data.Key] = d
	}
}

func getQueryResponse(key string, acc map[string]QueryResponse) QueryResponse {
	d, ok := acc[key]
	if !ok {
		d = QueryResponse{Montreal: nil}
	}
	return d
}

func main() {
	inputQueue, _ := common.InitializeRabbitQueue[Accumulator, Accumulator]("accumulator", "rabbit")
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		acc := make(map[string]QueryResponse)
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
