package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

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

type JoinerDataStation struct {
	DataStation *SendableDataStation `json:"stationData,omitempty"`
	DataTrip    *SendableDataTrip    `json:"tripData,omitempty"`
	Name        string               `json:"name"`
	Key         string               `json:"key"`
	EOF         bool                 `json:"EOF"`
}

type sData struct {
	Lat  string
	Long string
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
	r := math.Acos(math.Sin(oLat)*math.Sin(fLat)+math.Cosh(oLat)*math.Cos(fLat)+2*math.Cos(fLong-oLong)) * 6371
	return r, nil
}

type AccumulatorData struct {
	EndingStation string  `json:"ending_station"`
	Distance      float64 `json:"distance"`
	EOF           bool    `json:"eof"`
}

func processData(station JoinerDataStation, accumulator map[string]sData, aq common.Queue[AccumulatorData, AccumulatorData]) {
	if station.DataStation != nil {
		if station.EOF {
			// trigger trip process
		}
		accumulator[station.DataStation.Name] = sData{Long: station.DataStation.Longitude, Lat: station.DataStation.Latitude}
	} else if trip := station.DataTrip; trip != nil {
		if station.EOF {
			aq.SendMessage(AccumulatorData{
				EndingStation: "",
				Distance:      0,
				EOF:           true,
			})
			return
		}
		dOStation, ok := accumulator[trip.OStation]
		dEStation, oke := accumulator[trip.EStation]
		if !(ok && oke) {
			return
		}
		distance, err := dOStation.calcDistance(dEStation)
		if err != nil {
			return
		}
		if err := aq.SendMessage(AccumulatorData{
			EndingStation: trip.EStation,
			Distance:      distance,
		}); err != nil {
			// Fail horribly
		}

	}
}

func main() {

	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("stationWorkers", "rabbit")
	aq, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("preAccumulatorMontreal", "rabbit")
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		acc := make(map[string]sData)
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc, aq)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
