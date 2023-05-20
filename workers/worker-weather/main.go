package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"strconv"
	"strings"
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
	if weather.Data.Prec < 30 {
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
	}
}

func main() {
	id := os.Getenv("id")
	distributors, err := strconv.Atoi(os.Getenv("distributors"))
	common.FailOnError(err, "missing env value of distributors")
	inputQueue, _ := common.InitializeRabbitQueue[WorkerWeather, WorkerWeather]("weatherWorkers", "rabbit", id, 0)
	outputQueueWeather, _ := common.InitializeRabbitQueue[JoinerData, JoinerData]("weatherQueue", "rabbit", "", 0)
	v := make([]common.NextToNotify, 1)
	v = append(v, common.NextToNotify{
		Name:       "weatherQueue",
		Connection: outputQueueWeather,
	})
	iqEOF, _ := common.CreateConsumerEOF(v, "weatherWorkers", inputQueue, distributors)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer iqEOF.Close()
	defer inputQueue.Close()
	defer outputQueueWeather.Close()

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
	common.WaitForSigterm(grace)
}
