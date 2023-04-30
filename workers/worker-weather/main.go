package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Weather struct {
	Date string  `json:"date"`
	Prec float64 `json:"prec"`
}

type WorkerWeather struct {
	City string  `json:"city"`
	Data Weather `json:"data,omitempty"`
	Key  string  `json:"key"`
	EOF  bool    `json:"EOF"`
}

type SendableDataWeather struct {
	Date string  `json:"day"`
	Prec float64 `json:"prec"`
}

type JoinerData struct {
	City string              `json:"city"`
	Data SendableDataWeather `json:"weatherData"`
	Key  string              `json:"key"`
	EOF  bool                `json:"EOF"`
}

func processData(
	weather WorkerWeather,
	qt common.Queue[JoinerData, JoinerData]) {
	err := qt.SendMessage(JoinerData{
		City: weather.City,
		Key:  weather.Key,
		EOF:  weather.EOF,
		Data: SendableDataWeather{
			Date: weather.Data.Date,
			Prec: weather.Data.Prec,
		},
	})
	if err != nil {
		common.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
		// ToDo implement shutDown manager
	}
}

func main() {

	inputQueue, _ := common.InitializeRabbitQueue[WorkerWeather, WorkerWeather]("weatherWorkers", "rabbit")
	outputQueueWeather, _ := common.InitializeRabbitQueue[JoinerData, JoinerData]("weatherQueue", "rabbit")

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
			processData(data, outputQueueWeather)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-cancelChan
	log.Printf("Closing for sigterm received")
}
