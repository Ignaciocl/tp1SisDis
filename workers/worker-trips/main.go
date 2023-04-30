package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Trip struct {
	OStation string `json:"o_station"`
	EStation string `json:"e_station"`
	Duration int32  `json:"duration"`
	Year     int    `json:"year"`
	Date     string `json:"day"`
}

type WorkerTrip struct {
	City string `json:"city"`
	Data Trip   `json:"data,omitempty"`
	Key  string `json:"key"`
	EOF  bool   `json:"EOF"`
}

type SendableDataMontreal struct {
	OStation string `json:"o_station"`
	EStation string `json:"e_station"`
}

type SendableDataAvg struct {
	Station string `json:"station"`
	Year    int    `json:"year"`
}

type SendableDataWeather struct {
	Duration int32  `json:"duration"`
	Date     string `json:"day"`
}

type JoinerData[T any] struct {
	Data T      `json:"data"`
	Key  string `json:"key"`
	EOF  bool   `json:"EOF"`
	City string `json:"city,omitempty"`
}

const MontrealStation = "Montreal"

func processData(
	trip WorkerTrip,
	qm common.Queue[JoinerData[SendableDataMontreal], JoinerData[SendableDataMontreal]],
	qs common.Queue[JoinerData[SendableDataAvg], JoinerData[SendableDataAvg]],
	qt common.Queue[JoinerData[SendableDataWeather], JoinerData[SendableDataWeather]]) {
	if trip.City == MontrealStation {
		err := qm.SendMessage(JoinerData[SendableDataMontreal]{
			Key: trip.Key,
			EOF: trip.EOF,
			Data: SendableDataMontreal{
				OStation: trip.Data.OStation,
				EStation: trip.Data.EStation,
			},
		})
		if err != nil {
			common.FailOnError(err, "Couldn't send message to joiner montreal, failing horribly")
			// ToDo implement shutDown manager
		}
	}
	if trip.Data.Year == 2016 || trip.Data.Year == 2017 {
		err := qs.SendMessage(JoinerData[SendableDataAvg]{
			Key: trip.Key,
			EOF: trip.EOF,
			Data: SendableDataAvg{
				Station: trip.Data.OStation,
				Year:    trip.Data.Year,
			},
		})
		if err != nil {
			common.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
			// ToDo implement shutDown manager
		}
	}
	err := qt.SendMessage(JoinerData[SendableDataWeather]{
		Key:  trip.Key,
		EOF:  trip.EOF,
		City: trip.City,
		Data: SendableDataWeather{
			Duration: trip.Data.Duration,
			Date:     trip.Data.Date,
		},
	})
	if err != nil {
		common.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
		// ToDo implement shutDown manager
	}
}

func main() {

	inputQueue, _ := common.InitializeRabbitQueue[WorkerTrip, WorkerTrip]("stationWorkers", "rabbit")
	outputQueueMontreal, _ := common.InitializeRabbitQueue[JoinerData[SendableDataMontreal], JoinerData[SendableDataMontreal]]("montrealQueue", "rabbit")
	outputQueueStations, _ := common.InitializeRabbitQueue[JoinerData[SendableDataAvg], JoinerData[SendableDataAvg]]("stationsQueue", "rabbit")
	outputQueueWeather, _ := common.InitializeRabbitQueue[JoinerData[SendableDataWeather], JoinerData[SendableDataWeather]]("weatherQueue", "rabbit")

	cancelChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, outputQueueMontreal, outputQueueStations, outputQueueWeather)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-cancelChan
	log.Printf("Closing for sigterm received")
}
