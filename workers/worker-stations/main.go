package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strconv"
	"syscall"
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

func processData(station WorkerStation, qm, qs common.Queue[JoinerDataStation, JoinerDataStation]) {
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
		err := qm.SendMessage(js)
		if err != nil {
			common.FailOnError(err, "Couldn't send message to joiner montreal, failing horribly")
			// ToDo implement shutDown manager
		}
	}
	if station.Data.Year == 2016 || station.Data.Year == 2017 {
		err := qs.SendMessage(js)
		if err != nil {
			common.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
			// ToDo implement shutDown manager
		}
	}
}

func main() {
	id := os.Getenv("id")
	distributors, err := strconv.Atoi(os.Getenv("distributors"))
	common.FailOnError(err, "missing env value of distributors")
	inputQueue, _ := common.InitializeRabbitQueue[WorkerStation, WorkerStation]("stationWorkers", "rabbit", id, 0)
	outputQueueMontreal, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("montrealQueue", "rabbit", "", 0)
	outputQueueStations, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("stationsQueue", "rabbit", "", 0)
	v := make([]common.NextToNotify, 2)
	v = append(v, common.NextToNotify{
		Name:       "montrealQueue",
		Connection: outputQueueMontreal,
	}, common.NextToNotify{
		Name:       "stationsQueue",
		Connection: outputQueueStations,
	})
	iqEOF, err := common.CreateConsumerEOF(v, "stationWorkers", inputQueue, distributors)
	common.FailOnError(err, "could not use consumer")
	defer iqEOF.Close()
	defer inputQueue.Close()
	defer outputQueueMontreal.Close()
	defer outputQueueStations.Close()

	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			if data.EOF {
				log.Infof("eof received to be triggered: %v", data)
				iqEOF.AnswerEofOk(data.IdempotencyKey, nil)
				continue
			}
			processData(data, outputQueueMontreal, outputQueueStations)
		}
	}()

	log.Info(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
	log.Print("Closing for sigterm received")
}
