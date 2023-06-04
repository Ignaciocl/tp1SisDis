package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	"log"
	"os"
	"strconv"
	"strings"
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
	Data []Trip `json:"data,omitempty"`
	Key  string `json:"key"`
	common.EofData
}

type SendableDataMontreal struct {
	OStation string `json:"o_station"`
	EStation string `json:"e_station"`
	Year     int    `json:"year"`
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
	Data []T    `json:"tripData"`
	Key  string `json:"key"`
	EOF  bool   `json:"EOF"`
	City string `json:"city,omitempty"`
}

const MontrealStation = "montreal"

func processData(
	trip WorkerTrip,
	qm queue.Sender[JoinerData[SendableDataMontreal]],
	qs queue.Sender[JoinerData[SendableDataAvg]],
	qt queue.Sender[JoinerData[SendableDataWeather]]) {
	buildMontreal := trip.City == MontrealStation
	batchSize := len(trip.Data)
	vm := make([]SendableDataMontreal, 0, batchSize)
	vy := make([]SendableDataAvg, 0, batchSize)
	va := make([]SendableDataWeather, 0, batchSize)
	for _, v := range trip.Data {
		if buildMontreal {
			vm = append(vm, SendableDataMontreal{
				OStation: v.OStation,
				EStation: v.EStation,
				Year:     v.Year,
			})
		}
		if v.Year == 2016 || v.Year == 2017 {
			vy = append(vy, SendableDataAvg{
				Station: v.OStation,
				Year:    v.Year,
			})
		}
		if v.Duration <= 0 {
			v.Duration = 0
		}
		va = append(va, SendableDataWeather{
			Duration: int32(v.Duration),
			Date:     getDate(v.Date),
		})
	}

	if trip.City == MontrealStation {
		err := qm.SendMessage(JoinerData[SendableDataMontreal]{
			Key:  trip.Key,
			EOF:  trip.EOF,
			Data: vm,
		})
		if err != nil {
			utils.FailOnError(err, "Couldn't send message to joiner montreal, failing horribly")
		}
	}
	err := qs.SendMessage(JoinerData[SendableDataAvg]{
		Key:  trip.Key,
		EOF:  trip.EOF,
		Data: vy,
		City: trip.City,
	})
	if err != nil {
		utils.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
	}
	err = qt.SendMessage(JoinerData[SendableDataWeather]{
		Key:  trip.Key,
		EOF:  trip.EOF,
		City: trip.City,
		Data: va,
	})
	if err != nil {
		utils.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
	}
}

func main() {

	id := os.Getenv("id")
	distributors, err := strconv.Atoi(os.Getenv("distributors"))
	utils.FailOnError(err, "missing env value of distributors")
	inputQueue, _ := queue.InitializeReceiver[WorkerTrip]("tripWorkers", "rabbit", id, "", nil)
	outputQueueMontreal, _ := queue.InitializeSender[JoinerData[SendableDataMontreal]]("montrealQueueTrip", distributors, nil, "rabbit")
	outputQueueStations, _ := queue.InitializeSender[JoinerData[SendableDataAvg]]("stationsQueueTrip", distributors, nil, "rabbit")
	outputQueueWeather, _ := queue.InitializeSender[JoinerData[SendableDataWeather]]("weatherQueueTrip", distributors, nil, "rabbit")
	v := make([]common.NextToNotify, 0, 3)
	v = append(v, common.NextToNotify{
		Name:       "montrealQueueTrip",
		Connection: outputQueueMontreal,
	}, common.NextToNotify{
		Name:       "stationsQueueTrip",
		Connection: outputQueueStations,
	}, common.NextToNotify{
		Name:       "weatherQueueTrip",
		Connection: outputQueueWeather,
	})
	iqEOF, _ := common.CreateConsumerEOF(v, "tripWorkers", inputQueue, distributors)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer utils.RecoverFromPanic(grace, "")
	defer iqEOF.Close()
	defer inputQueue.Close()
	defer outputQueueMontreal.Close()
	defer outputQueueStations.Close()
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
			processData(data, outputQueueMontreal, outputQueueStations, outputQueueWeather)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	utils.WaitForSigterm(grace)
}
