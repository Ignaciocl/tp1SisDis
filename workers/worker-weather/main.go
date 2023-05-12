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
	if weather.Data.Prec < 60 {
		return
	}
	err := qt.SendMessage(JoinerData{
		City: weather.City,
		Key:  weather.Key,
		EOF:  weather.EOF,
		Data: SendableDataWeather{
			Date: getDate(weather.Data.Date),
			Prec: weather.Data.Prec,
		},
	})
	if err != nil {
		common.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
		// ToDo implement shutDown manager
	}
}

type checker struct {
	data      map[string]string
	blocker   chan struct{}
	q         common.Queue[WorkerWeather, WorkerWeather]
	filesUsed int
}

func (c checker) IsStillUsingNecessaryDataForFile(file string, city string) bool {
	if city != "washington" {
		<-c.blocker
	} else {
		for {
			<-c.blocker
			if c.q.IsEmpty() {
				break
			}
		}
	}
	return true
}

func main() {

	inputQueue, _ := common.InitializeRabbitQueue[WorkerWeather, WorkerWeather]("weatherWorkers", "rabbit")
	outputQueueWeather, _ := common.InitializeRabbitQueue[JoinerData, JoinerData]("weatherQueue", "rabbit")
	wfe, _ := common.CreateConsumerEOF("rabbit", "workerWeather")
	defer inputQueue.Close()
	defer outputQueueWeather.Close()
	defer wfe.Close()

	cancelChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)
	eofCheck := map[string]string{}
	eofCheck["city"] = ""
	blocker := make(chan struct{}, 1)
	blocker <- struct{}{}
	c := checker{data: eofCheck, blocker: blocker, filesUsed: 0, q: inputQueue}
	go func() {
		wfe.AnswerEofOk(c)
	}()
	go func() {
		pCity := "montreal"
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			if data.City != pCity || pCity == "washington" {
				pCity = data.City
				blocker <- struct{}{}
			}
			processData(data, outputQueueWeather)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-cancelChan
	log.Printf("Closing for sigterm received")
}
