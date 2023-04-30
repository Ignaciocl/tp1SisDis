package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Station struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Year      int    `json:"year"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
}

type WorkerStation struct {
	City string  `json:"city"`
	Data Station `json:"data,omitempty"`
	Key  string  `json:"key"`
	EOF  bool    `json:"EOF"`
}

type SendableDataStation struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
}

type JoinerDataStation struct {
	Data SendableDataStation `json:"stationData"`
	Name string              `json:"name"`
	Key  string              `json:"key"`
	EOF  bool                `json:"EOF"`
}

const MontrealStation = "Montreal"

func processData(station WorkerStation, qm, qs common.Queue[JoinerDataStation, JoinerDataStation]) {
	js := JoinerDataStation{
		Data: SendableDataStation{
			Code:      station.Data.Code,
			Name:      station.Data.Name,
			Longitude: station.Data.Longitude,
			Latitude:  station.Data.Latitude,
		},
		Key: station.Key,
		EOF: station.EOF,
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

	inputQueue, _ := common.InitializeRabbitQueue[WorkerStation, WorkerStation]("stationWorkers", "rabbit")
	outputQueueMontreal, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("montrealQueue", "rabbit")
	outputQueueStations, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("stationsQueue", "rabbit")

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
			processData(data, outputQueueMontreal, outputQueueStations)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
	log.Printf("Closing for sigterm received")
}
