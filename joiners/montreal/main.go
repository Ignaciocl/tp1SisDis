package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	lasPistasDeBlue "github.com/umahmood/haversine"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

var StationEof = false

type SendableDataStation struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
}

type SendableDataTrip struct {
	OStation string `json:"o_station"`
	EStation string `json:"e_station"`
}

var TRIPS = make([]SendableDataTrip, 0)

type JoinerDataStation struct {
	DataStation *SendableDataStation `json:"stationData,omitempty"`
	DataTrip    *SendableDataTrip    `json:"tripData,omitempty"`
	Name        string               `json:"name,omitempty"`
	Key         string               `json:"key,omitempty"`
	common.EofData
}

type sData struct {
	Lat  string
	Long string
	Name string
}

func (o sData) calcDistance(f sData) (float64, error) {
	oLong, err := strconv.ParseFloat(o.Long, 8)
	if err != nil {
		return 0, err
	}
	oLat, err := strconv.ParseFloat(o.Lat, 8)
	if err != nil {
		return 0, err
	}
	fLong, err := strconv.ParseFloat(f.Long, 8)
	if err != nil {
		return 0, err
	}
	fLat, err := strconv.ParseFloat(f.Lat, 8)
	if err != nil {
		return 0, err
	}
	origin := lasPistasDeBlue.Coord{
		Lat: oLat,
		Lon: oLong,
	}
	destiny2TheRevengeOfDestiny := lasPistasDeBlue.Coord{
		Lat: fLat,
		Lon: fLong,
	}
	_, km := lasPistasDeBlue.Distance(origin, destiny2TheRevengeOfDestiny)
	return km, nil
}

type AccumulatorData struct {
	EndingStation string  `json:"ending_station"`
	Distance      float64 `json:"distance"`
	EOF           bool    `json:"eof,omitempty"`
}

func processData(station JoinerDataStation, accumulator map[string]sData, aq common.Queue[AccumulatorData, AccumulatorData]) {
	if s := station.DataStation; s != nil {
		accumulator[station.DataStation.Code] = sData{Long: station.DataStation.Longitude, Lat: station.DataStation.Latitude, Name: station.DataStation.Name}
	} else if trip := station.DataTrip; trip != nil {
		SendInfo(accumulator, *trip, aq)
	}
}

func SendInfo(accumulator map[string]sData, trip SendableDataTrip, aq common.Queue[AccumulatorData, AccumulatorData]) bool {
	dOStation, ok := accumulator[trip.OStation]
	dEStation, oke := accumulator[trip.EStation]
	if !(ok && oke) {
		return true
	}
	distance, err := dOStation.calcDistance(dEStation)
	if err != nil {
		return true
	}

	if err := aq.SendMessage(AccumulatorData{
		EndingStation: dEStation.Name,
		Distance:      distance,
	}); err != nil {
		common.FailOnError(err, "some error happened while sending")
	}
	return false
}

type actionable struct {
	c  chan struct{}
	nc chan struct{}
}

func (a actionable) DoActionIfEOF() {
	a.nc <- <-a.c // continue the loop
}

func main() {
	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("montrealQueue", "rabbit", "", 0)
	inputQueueTrip, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("montrealQueueTrip", "rabbit", "", 0)
	aq, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("preAccumulatorMontreal", "rabbit", "", 0)
	sfe, _ := common.CreateConsumerEOF(nil, "montrealQueueEOF", inputQueue, 3)
	tfe, _ := common.CreateConsumerEOF([]string{"preAccumulatorMontrealEOF"}, "montrealQueueTripEOF", inputQueueTrip, 3)
	defer inputQueue.Close()
	defer aq.Close()
	defer inputQueueTrip.Close()
	oniChan := make(chan os.Signal, 1)
	tt := make(chan struct{}, 1)
	st := make(chan struct{}, 1)
	st <- struct{}{}
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	acc := make(map[string]sData)
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
			d := <-st
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc, aq)
			st <- d
		}
	}()
	go func() {
		for {
			data, err := inputQueueTrip.ReceiveMessage()
			if data.EOF {

				tfe.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  tt,
					nc: st,
				})
				continue
			}
			d := <-tt
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc, aq)
			tt <- d
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
