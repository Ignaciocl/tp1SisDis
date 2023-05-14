package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func getDate(date string) string {
	return strings.Split(date, " ")[0]
}

type Trip struct {
	OStation string  `json:"start_station_code"`
	EStation string  `json:"end_station_code"`
	Duration float64 `json:"duration_sec,string"`
	Year     int     `json:"yearid,string"`
	Date     string  `json:"start_date"`
}

type WorkerTrip struct {
	City string `json:"city"`
	Data Trip   `json:"data,omitempty"`
	Key  string `json:"key"`
	common.EofData
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
	Date     string `json:"date"`
}

type JoinerData[T any] struct {
	Data T      `json:"tripData"`
	Key  string `json:"key"`
	EOF  bool   `json:"EOF"`
	City string `json:"city,omitempty"`
}

const MontrealStation = "montreal"

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
		d := SendableDataAvg{
			Station: trip.Data.OStation,
			Year:    trip.Data.Year,
		}
		err := qs.SendMessage(JoinerData[SendableDataAvg]{
			Key:  trip.Key,
			EOF:  trip.EOF,
			Data: d,
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
			Duration: int32(trip.Data.Duration),
			Date:     getDate(trip.Data.Date),
		},
	})
	if err != nil {
		common.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
		// ToDo implement shutDown manager
	}
}

func main() {

	id := os.Getenv("id")
	inputQueue, _ := common.InitializeRabbitQueue[WorkerTrip, WorkerTrip]("tripWorkers", "rabbit", id, 0)
	outputQueueMontreal, _ := common.InitializeRabbitQueue[JoinerData[SendableDataMontreal], JoinerData[SendableDataMontreal]]("montrealQueueTrip", "rabbit", "", 0)
	outputQueueStations, _ := common.InitializeRabbitQueue[JoinerData[SendableDataAvg], JoinerData[SendableDataAvg]]("stationsQueueTrip", "rabbit", "", 0)
	outputQueueWeather, _ := common.InitializeRabbitQueue[JoinerData[SendableDataWeather], JoinerData[SendableDataWeather]]("weatherQueueTrip", "rabbit", "", 0)
	v := make([]string, 3)
	v = append(v, "montrealQueueTripEOF", "stationsQueueTripEOF", "weatherQueueTripEOF")
	iqEOF, _ := common.CreateConsumerEOF(v, "tripWorkersEOF", inputQueue, 3)
	defer iqEOF.Close()
	defer inputQueue.Close()
	defer outputQueueMontreal.Close()
	defer outputQueueStations.Close()
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
			processData(data, outputQueueMontreal, outputQueueStations, outputQueueWeather)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-cancelChan
	log.Printf("Closing for sigterm received")
}
