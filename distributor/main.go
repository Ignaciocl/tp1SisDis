package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type receivedData struct {
	Stations []string `json:"stationData,omitempty"`
	Trips    []string `json:"tripsData,omitempty"`
	Weather  []string `json:"weather,omitempty"`
	City     string   `json:"city,omitempty"`
}

type SendableData struct {
	City string `json:"city"`
	Data string `json:"data,omitempty"` // Why would i decompress here? it is just a distributor
	Key  string `json:"key"`
	EOF  bool   `json:"EOF"`
}

func SendMessagesToQueue(data []string, queue common.Queue[SendableData, SendableData], city string) {
	for _, v := range data {
		queue.SendMessage(SendableData{
			City: city,
			Data: v,
			Key:  "random",
			EOF:  false,
		})
	}
}

func main() {

	inputQueue, _ := common.InitializeRabbitQueue[receivedData, receivedData]("distributor", "rabbit")
	weatherQueue, _ := common.InitializeRabbitQueue[SendableData, SendableData]("weatherWorkers", "rabbit")
	tripQueue, _ := common.InitializeRabbitQueue[SendableData, SendableData]("tripWorkers", "rabbit")
	stationQueue, _ := common.InitializeRabbitQueue[SendableData, SendableData]("stationWorkers", "rabbit")

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
			if data.Weather != nil {
				SendMessagesToQueue(data.Weather, weatherQueue, data.City)
			} else if data.Trips != nil {
				SendMessagesToQueue(data.Trips, tripQueue, data.City)
			} else if data.Stations != nil {
				SendMessagesToQueue(data.Stations, stationQueue, data.City)
			}
		}
	}()
	<-cancelChan
	log.Printf("Closing for sigterm received")
}
