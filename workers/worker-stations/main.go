package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

type Station struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Year      int    `json:"yearid,string"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
}

type WorkerStation struct {
	City string  `json:"city"`
	Data Station `json:"data,omitempty"`
	Key  string  `json:"key"`
	common.EofData
}

type SendableDataStation struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
	Year      int    `json:"year"`
}

type JoinerDataStation struct {
	Data SendableDataStation `json:"stationData"`
	Name string              `json:"name"`
	Key  string              `json:"key"`
	EOF  bool                `json:"EOF"`
	City string              `json:"city"`
}

const MontrealStation = "montreal"

func processData(station WorkerStation, qm, qs queue.Sender[JoinerDataStation]) {
	js := JoinerDataStation{
		Data: SendableDataStation{
			Code:      station.Data.Code,
			Name:      station.Data.Name,
			Longitude: station.Data.Longitude,
			Latitude:  station.Data.Latitude,
			Year:      station.Data.Year,
		},
		City: station.City,
		Key:  station.Key,
		EOF:  station.EOF,
	}
	if station.City == MontrealStation {
		err := qm.SendMessage(js, "")
		if err != nil {
			utils.FailOnError(err, "Couldn't send message to joiner montreal, failing horribly")
		}
	}
	if station.Data.Year == 2016 || station.Data.Year == 2017 {
		err := qs.SendMessage(js, "")
		if err != nil {
			utils.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
		}
	}
}

func main() {
	id := os.Getenv("id")
	distributors, err := strconv.Atoi(os.Getenv("distributors"))
	utils.FailOnError(err, "missing env value of distributors")
	inputQueue, _ := queue.InitializeReceiver[WorkerStation]("stationWorkers", "rabbit", id, "", nil)
	outputQueueMontreal, _ := queue.InitializeSender[JoinerDataStation]("montrealQueue", 0, nil, "rabbit")
	outputQueueStations, _ := queue.InitializeSender[JoinerDataStation]("stationsQueue", 0, nil, "rabbit")
	v := make([]common.NextToNotify, 2)
	v = append(v, common.NextToNotify{
		Name:       "montrealQueue",
		Connection: outputQueueMontreal,
	}, common.NextToNotify{
		Name:       "stationsQueue",
		Connection: outputQueueStations,
	})
	iqEOF, err := common.CreateConsumerEOF(v, "stationWorkers", inputQueue, distributors)
	utils.FailOnError(err, "could not use consumer")
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer iqEOF.Close()
	defer inputQueue.Close()
	defer outputQueueMontreal.Close()
	defer outputQueueStations.Close()

	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			if data.EOF {
				log.Infof("eof received to be triggered: %v", data)
				iqEOF.AnswerEofOk(data.IdempotencyKey, nil)
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			processData(data, outputQueueMontreal, outputQueueStations)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()

	log.Info(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}
