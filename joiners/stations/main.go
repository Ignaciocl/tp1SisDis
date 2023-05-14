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
	Code string `json:"code"`
	Name string `json:"name"`
}

type ReceivableDataTrip struct {
	Station string `json:"station"`
	Year    int    `json:"year"`
}

type JoinerDataStation struct {
	DataStation *ReceivableDataStation `json:"stationData,omitempty"`
	DataTrip    *[]ReceivableDataTrip  `json:"tripData,omitempty"`
	Name        string                 `json:"name"`
	Key         string                 `json:"key"`
	common.EofData
}

type AccumulatorData struct {
	AvgStations []string `json:"avg_stations"`
	Key         string   `json:"key"`
	common.EofData
}

type stationData struct {
	sweetSixteen int
	sadSeventeen int
	name         string
}

type weird struct {
	m map[string]stationData
}

func (sd stationData) wasDouble() bool {
	return sd.sadSeventeen > 2*sd.sweetSixteen
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

func processData(data JoinerDataStation, w *weird) {
	accumulator := w.m
	if station := data.DataStation; station != nil {
		sData, _ := getStationData(station.Code, accumulator)
		sData.name = station.Name
		accumulator[station.Code] = sData
	} else if trips := data.DataTrip; trips != nil {
		for _, trip := range *trips {
			dStation, err := getStationData(trip.Station, accumulator)
			if err != nil {

				return
			}
			if trip.Year == 2016 {
				dStation.sweetSixteen += 1
			} else if trip.Year == 2017 {
				dStation.sadSeventeen += 1
			}
			accumulator[trip.Station] = dStation
		}
	}
}

type actionable struct {
	c  chan struct{}
	nc chan struct{}
}

func (a actionable) DoActionIfEOF() {
	a.nc <- <-a.c // continue the loop
}

func main() {

	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("stationsQueue", "rabbit", "", 0)
	inputQueueTrip, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("stationsQueueTrip", "rabbit", "", 0)
	aq, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("accumulator", "rabbit", "", 0)
	sfe, _ := common.CreateConsumerEOF(nil, "stationsQueue", inputQueue, 3)
	tfe, _ := common.CreateConsumerEOF(nil, "stationsQueueTrip", inputQueueTrip, 3)
	defer sfe.Close()
	defer tfe.Close()
	defer inputQueue.Close()
	defer aq.Close()
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	ns := make(chan struct{}, 1)
	tt := make(chan struct{}, 1)
	st := make(chan struct{}, 1)
	st <- struct{}{}
	acc := map[string]stationData{}
	w := weird{m: acc}
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			if data.EOF {

				sfe.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  st,
					nc: tt,
				})
				continue
			}
			p := <-st
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, &w)
			st <- p
		}
	}()
	go func() {
		for {
			data, err := inputQueueTrip.ReceiveMessage()
			if data.EOF {
				log.Printf("joiner station trip eof received")
				tfe.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  tt,
					nc: ns,
				})
				continue
			}
			p := <-tt
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, &w)
			tt <- p
		}
	}()
	go func() {
		savedData := make(map[string]struct{}, 0)
		for i := 0; i < 3; i += 1 {
			<-ns
			acc = w.m
			for _, value := range acc {
				if value.wasDouble() && value.name != "" {
					savedData[value.name] = struct{}{}

				}
			}

			if i != 2 {
				st <- struct{}{}
			}
		}
		v := make([]string, 0, len(savedData))
		for key, _ := range savedData {
			v = append(v, key)
		}
		l := AccumulatorData{
			AvgStations: v,
			Key:         "random",
		}

		_ = aq.SendMessage(l)
		eof := AccumulatorData{EofData: common.EofData{
			EOF:            true,
			IdempotencyKey: "random",
		},
		}
		aq.SendMessage(eof)
		st <- struct{}{}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
