package main

import (
	"encoding/json"
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
	EOF         bool                 `json:"EOF,omitempty"`
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
		log.Printf("station received in montreal is: %v", s)
		accumulator[station.DataStation.Code] = sData{Long: station.DataStation.Longitude, Lat: station.DataStation.Latitude, Name: station.DataStation.Name}
	} else if trip := station.DataTrip; trip != nil {
		log.Printf("trip received in montreal is: %v", trip)
		if !StationEof {
			TRIPS = append(TRIPS, *trip)
			return
		}
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

type checker struct {
	data    map[string]string
	blocker chan struct{}
	station chan struct{}
}

func (c checker) IsStillUsingNecessaryDataForFile(file string, city string) bool {
	d := <-c.blocker
	if file == "stations" {
		StationEof = true
		c.station <- struct{}{}
	}
	c.blocker <- d
	return true
}

func main() {
	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("montrealQueue", "rabbit")
	aq, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("preAccumulatorMontreal", "rabbit")
	wfe, _ := common.CreateConsumerEOF("rabbit", "joinerMontreal")
	defer inputQueue.Close()
	defer aq.Close()
	defer wfe.Close()
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	eofCheck := map[string]string{}
	eofCheck["city"] = ""
	blocker := make(chan struct{}, 1)
	stationFinished := make(chan struct{}, 1)
	blocker <- struct{}{}
	c := checker{data: eofCheck, blocker: blocker, station: stationFinished}
	go func() {
		wfe.AnswerEofOk(c)
	}()
	acc := make(map[string]sData)
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			d := <-blocker
			processData(data, acc, aq)
			p, _ := json.Marshal(data)
			log.Printf("DATA IN MONTREAL RECEIVED IS: %s\n", string(p))
			blocker <- d
		}
	}()
	go func() {
		<-stationFinished
		d := <-blocker
		for _, t := range TRIPS {
			SendInfo(acc, t, aq)
		}
		log.Printf("montreal info trips is: %v\n", TRIPS)
		blocker <- d
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
