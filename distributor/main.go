package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
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

type SendableDataTrip struct {
	City string        `json:"city"`
	Data []interface{} `json:"data,omitempty"` // Why would i decompress here? it is just a distributor
	Key  string        `json:"key"`
	EOF  bool          `json:"EOF"`
}

func SendMessagesToQueue(data []interface{}, queue queue.Sender[SendableData], city string) {
	for _, v := range data {
		err := queue.SendMessage(SendableData{
			City: city,
			Data: v,
			Key:  "random",
			EOF:  false,
		}, "")
		if err != nil {

		}
	}
}

func main() {
	id := os.Getenv("id")
	inputQueue, _ := queue.InitializeReceiver[receivedData]("distributor", "rabbit", id, "", nil)
	wq, _ := queue.InitializeSender[SendableData]("weatherWorkers", 3, nil, "rabbit")
	tq, _ := queue.InitializeSender[SendableDataTrip]("tripWorkers", 3, nil, "rabbit")
	sq, _ := queue.InitializeSender[SendableData]("stationWorkers", 3, nil, "rabbit")
	wqe, _ := common.CreateConsumerEOF([]common.NextToNotify{{Name: "weatherWorkers", Connection: wq}}, "distributor", inputQueue, 1)
	tqe, _ := common.CreateConsumerEOF([]common.NextToNotify{{Name: "tripWorkers", Connection: tq}}, "distributor", inputQueue, 1)
	sqe, _ := common.CreateConsumerEOF([]common.NextToNotify{{Name: "stationWorkers", Connection: sq}}, "distributor", inputQueue, 1)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer wqe.Close()
	defer inputQueue.Close()
	defer wq.Close()
	defer tq.Close()
	defer sq.Close()
	// catch SIGETRM or SIGINTERRUPT
	go func() {
		pFile := ""
		var sender queue.Sender[SendableData]
		var me common.WaitForEof
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
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
					utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
					continue
				}

				log.Infof("eof received and distributed for %v", data)
				me.AnswerEofOk(data.IdempotencyKey, nil)
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			if pFile != data.File {
				pFile = data.File
				if pFile == "weather" {
					sender = wq
				} else if pFile == "stations" {
					sender = sq
				}
			}
			if pFile == "trips" {
				tq.SendMessage(SendableDataTrip{
					City: data.City,
					Data: data.Data,
					Key:  "random",
					EOF:  false,
				}, "")
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			SendMessagesToQueue(data.Data, sender, data.City)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()
	common.WaitForSigterm(grace)
}
