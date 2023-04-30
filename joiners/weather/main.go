package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type ReceivableDataWeather struct {
	Date string `json:"date"`
}

type ReceivableDataTrip struct {
	Date     string `json:"date"`
	Duration int    `json:"duration"`
}

type JoinerDataStation struct {
	DataWeather *ReceivableDataWeather `json:"weatherData,omitempty"`
	DataTrip    *ReceivableDataTrip    `json:"tripData,omitempty"`
	Name        string                 `json:"name"`
	Key         string                 `json:"key"`
	EOF         bool                   `json:"EOF"`
}

type AccumulatorData struct {
	Dur float64 `json:"durationPrect"`
	Key string  `json:"key"`
}

type weatherDuration struct {
	total    int
	duration int
}

func (sd *weatherDuration) add(duration int) {
	sd.total += 1
	sd.duration += duration
}

func (sd *weatherDuration) addW(w weatherDuration) {
	sd.total += w.total
	sd.duration += w.duration
}

func processData(data JoinerDataStation, accumulator map[string]weatherDuration) { // Check for city
	if weather := data.DataWeather; weather != nil {
		w := weatherDuration{
			total:    0,
			duration: 0,
		}
		accumulator[weather.Date] = w
	} else if trip := data.DataTrip; trip != nil {
		if wd, ok := accumulator[trip.Date]; ok {
			wd.add(trip.Duration)
			accumulator[trip.Date] = wd
		}
	}
}

func main() {
	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("stationWorkers", "rabbit")
	aq, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("accumulator", "rabbit")
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		// cityMap := make(map[string]map[string]weatherDuration) follow this tomorrow
		acc := make(map[string]weatherDuration)
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			if data.EOF && data.DataTrip != nil {
				v := weatherDuration{
					total:    0,
					duration: 0,
				}
				for _, value := range acc {
					v.addW(value)
				}
				l := AccumulatorData{
					Dur: float64(v.duration) / float64(v.total),
					Key: data.Key,
				}
				_ = aq.SendMessage(l)
			} else {
				processData(data, acc)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
