package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const dateLayout = "2006-01-02"

func getDate(date string) string {
	d, err := time.Parse(dateLayout, date)
	if err != nil {
		common.FailOnError(err, "Invalid date while parsing")
	}
	return strings.Split(d.AddDate(0, 0, -1).String(), " ")[0]
}

type Weather struct {
	Date string  `json:"date"`
	Prec float64 `json:"prectot,string"`
}

type WorkerWeather struct {
	City string  `json:"city"`
	Data Weather `json:"data,omitempty"`
	Key  string  `json:"key"`
	common.EofData
}

type SendableDataWeather struct {
	Date string  `json:"date"`
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
	if weather.Data.Prec < 60 {
		return
	}
	d := JoinerData{
		City: weather.City,
		Key:  weather.Key,
		EOF:  weather.EOF,
		Data: SendableDataWeather{
			Date: getDate(weather.Data.Date),
		},
	}
	err := qt.SendMessage(d)
	if err != nil {
		common.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
		// ToDo implement shutDown manager
	}
}

func main() {
	id := os.Getenv("id")
	inputQueue, _ := common.InitializeRabbitQueue[WorkerWeather, WorkerWeather]("weatherWorkers", "rabbit", id, 0)
	outputQueueWeather, _ := common.InitializeRabbitQueue[JoinerData, JoinerData]("weatherQueue", "rabbit", "", 0)
	v := make([]common.NextToNotify, 1)
	v = append(v, common.NextToNotify{
		Name:       "weatherQueue",
		Connection: outputQueueWeather,
	})
	iqEOF, _ := common.CreateConsumerEOF(v, "weatherWorkers", inputQueue, 3)
	defer iqEOF.Close()
	defer inputQueue.Close()
	defer outputQueueWeather.Close()

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
			if data.EOF {
				iqEOF.AnswerEofOk(data.IdempotencyKey, nil)
				continue
			}
			processData(data, outputQueueWeather)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-cancelChan
	log.Printf("Closing for sigterm received")
}
