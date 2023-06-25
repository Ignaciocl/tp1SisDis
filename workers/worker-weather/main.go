package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
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
		utils.FailOnError(err, "Invalid date while parsing")
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
	qt queue.Sender[JoinerData]) {
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
	err := qt.SendMessage(d, weather.Key)
	if err != nil {
		utils.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
	}
}

func main() {
	id := os.Getenv("id")
	distributors, err := strconv.Atoi(os.Getenv("distributors"))
	utils.FailOnError(err, "missing env value of distributors")
	inputQueue, _ := queue.InitializeReceiver[WorkerWeather]("weatherWorkers", "rabbit", id, "", nil)
	outputQueueWeather, _ := queue.InitializeSender[JoinerData]("weatherQueue", 0, inputQueue, "")
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
			data, msgId, err := inputQueue.ReceiveMessage()
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			if data.EOF {
				iqEOF.AnswerEofOk(data.IdempotencyKey, nil)
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			processData(data, outputQueueWeather)
			inputQueue.AckMessage(msgId)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}
