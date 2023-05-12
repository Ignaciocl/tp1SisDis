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

type weird struct {
	m map[string]stationData
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

func processData(data JoinerDataStation, w *weird) {
	accumulator := w.m
	if station := data.DataStation; station != nil {
		sData, _ := getStationData(station.Code, accumulator)
		sData.name = station.Name
		accumulator[station.Code] = sData
	} else if trip := data.DataTrip; trip != nil {
		dStation, err := getStationData(trip.Station, accumulator)
		if err != nil {
			log.Printf("missing trip info for station: %v\n", trip)
			return
		}
		if trip.Year == 2016 {
			dStation.sweetSixteen += 1
		} else if trip.Year == 2017 {
			dStation.sadSeventeen += 1
		}
	}
}

type checker struct {
	data             map[string]string
	blocker          chan struct{}
	tripBlocker      chan struct{}
	stationBlocker   chan struct{}
	finishProcessing chan struct{}
}

func (c checker) IsStillUsingNecessaryDataForFile(file string, city string) bool {
	d := <-c.blocker
	if file == "stations" {
		c.stationBlocker <- struct{}{}
	}
	if file == "trips" {
		c.tripBlocker <- struct{}{}
	}
	c.blocker <- d
	<-c.finishProcessing
	return true
}

func main() {

	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("stationsQueue", "rabbit")
	aq, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("accumulator", "rabbit")
	wfe, _ := common.CreateConsumerEOF("rabbit", "joinerStations")
	defer inputQueue.Close()
	defer aq.Close()
	defer wfe.Close()
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	eofCheck := map[string]string{}
	eofCheck["city"] = ""
	blocker := make(chan struct{}, 1)
	sb := make(chan struct{}, 1)
	tb := make(chan struct{}, 1)
	fp := make(chan struct{}, 1)
	blocker <- struct{}{}
	c := checker{data: eofCheck, blocker: blocker, stationBlocker: sb, tripBlocker: tb, finishProcessing: fp}
	go func() {
		wfe.AnswerEofOk(c)
	}()
	acc := map[string]stationData{}
	w := weird{m: acc}
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			d := <-blocker // there is a possibility of a rc here, another lock would be fine
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, &w)
			blocker <- d
		}
	}()
	go func() {
		savedData := make(map[string]struct{}, 0)
		for i := 0; i < 3; i += 1 {
			<-sb
			fp <- struct{}{}
			<-tb
			d := <-blocker
			acc = w.m
			for _, value := range acc {
				if value.wasDouble() && value.name != "" {
					savedData[value.name] = struct{}{}
					log.Printf("value to check is: %v\n", value)
				}
			}
			//acc = make(map[string]stationData)
			fp <- struct{}{}
			blocker <- d
		}
		v := make([]string, 0, len(savedData))
		for key, _ := range savedData {
			v = append(v, key)
		}
		l := AccumulatorData{
			AvgStations: v,
			Key:         "random",
		}
		log.Printf("data is: %+v\n", l)
		_ = aq.SendMessage(l)
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
