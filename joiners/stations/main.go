package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/pkg/errors"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type ReceivableDataStation struct {
	Name string `json:"name"`
}

type ReceivableDataTrip struct {
	Station string `json:"station"`
	Year    int    `json:"year"`
}

type JoinerDataStation struct {
	DataStation *ReceivableDataStation `json:"stationData,omitempty"`
	DataTrip    *ReceivableDataTrip    `json:"tripData,omitempty"`
	Name        string                 `json:"name"`
	Key         string                 `json:"key"`
	EOF         bool                   `json:"EOF"`
}

type AccumulatorData struct {
	AvgStations []string `json:"avg_stations"`
	Key         string   `json:"key"`
}

type stationData struct {
	sweetSixteen int
	sadSeventeen int
	name         string
}

func (sd stationData) wasDouble() bool {
	return sd.sadSeventeen >= 2*sd.sweetSixteen
}
func getStationData(key string, accumulator map[string]stationData) (stationData, error) {
	data, ok := accumulator[key]
	var err error
	if !ok {
		data = stationData{
			sweetSixteen: 0,
			sadSeventeen: 0,
			name:         "",
		}
		err = errors.New("data does not exist")
	}
	return data, err
}

func processData(data JoinerDataStation, accumulator map[string]stationData) {
	if station := data.DataStation; station != nil {
		sData, _ := getStationData(station.Name, accumulator)
		sData.name = station.Name
		accumulator[station.Name] = sData
	} else if trip := data.DataTrip; trip != nil {
		station, err := getStationData(station.Name, accumulator)
		if err != nil {
			// thrown because not known
			return
		}
		if trip.Year == 2016 {
			station.sweetSixteen += 1
		} else if trip.Year == 2017 {
			station.sadSeventeen += 1
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
		acc := make(map[string]stationData)
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			if data.EOF && data.DataTrip != nil {
				v := make([]string, 0, len(acc))
				for key, value := range acc {
					if value.wasDouble() {
						v = append(v, key)
					}
				}
				l := AccumulatorData{
					AvgStations: v,
					Key:         data.Key,
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
