package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type receivedData struct {
	File string        `json:"file"`
	Data []interface{} `json:"data"`
	City string        `json:"city,omitempty"`
	common.EofData
}

type SendableData struct {
	City string      `json:"city"`
	Data interface{} `json:"data,omitempty"` // Why would i decompress here? it is just a distributor
	Key  string      `json:"key"`
	EOF  bool        `json:"EOF"`
}

func SendMessagesToQueue(data []interface{}, queue common.Queue[SendableData, SendableData], city string) {
	for _, v := range data {
		err := queue.SendMessage(SendableData{
			City: city,
			Data: v,
			Key:  "random",
			EOF:  false,
		})
		if err != nil {

		}
	}
}

func main() {
	id := os.Getenv("id")
	inputQueue, _ := common.InitializeRabbitQueue[receivedData, receivedData]("distributor", "rabbit", id, 0)
	wq, _ := common.InitializeRabbitQueue[SendableData, SendableData]("weatherWorkers", "rabbit", "", 3)
	tq, _ := common.InitializeRabbitQueue[SendableData, SendableData]("tripWorkers", "rabbit", "", 3)
	sq, _ := common.InitializeRabbitQueue[SendableData, SendableData]("stationWorkers", "rabbit", "", 3)
	wqe, _ := common.CreateConsumerEOF([]string{"weatherWorkersEOF"}, "distributorEOF", inputQueue, 1)
	tqe, _ := common.CreateConsumerEOF([]string{"tripWorkersEOF"}, "distributorEOF", inputQueue, 1)
	sqe, _ := common.CreateConsumerEOF([]string{"stationWorkersEOF"}, "distributorEOF", inputQueue, 1)
	cancelChan := make(chan os.Signal, 1)
	defer wqe.Close()
	defer inputQueue.Close()
	defer wq.Close()
	defer tq.Close()
	defer sq.Close()
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		pFile := ""
		var queue common.Queue[SendableData, SendableData]
		var me common.WaitForEof
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			if data.EOF {
				if strings.Contains(data.IdempotencyKey, "trips") {
					me = tqe
				} else if strings.Contains(data.IdempotencyKey, "stations") {
					me = sqe
				} else if strings.Contains(data.IdempotencyKey, "weather") {
					me = wqe
				} else {

					continue
				}

				me.AnswerEofOk(data.IdempotencyKey, nil)
				continue
			}
			if pFile != data.File {
				pFile = data.File
				if pFile == "weather" {
					queue = wq
				} else if pFile == "trips" {
					queue = tq
				} else if pFile == "stations" {
					queue = sq
				}
			}
			SendMessagesToQueue(data.Data, queue, data.City)
		}
	}()
	<-cancelChan
	log.Printf("Closing for sigterm received")
}
