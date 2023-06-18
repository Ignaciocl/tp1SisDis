package main

import (
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

type SendableDataStation struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
	Year      int    `json:"year"`
}

type SendableDataTrip struct {
	OStation string `json:"o_station"`
	EStation string `json:"e_station"`
	Year     int    `json:"year"`
}
type JoinerDataStation struct {
	DataStation *SendableDataStation `json:"stationData,omitempty"`
	DataTrip    *[]SendableDataTrip  `json:"tripData,omitempty"`
	Name        string               `json:"name,omitempty"`
	Key         string               `json:"key,omitempty"`
	common.EofData
}

type sData struct {
	Lat  string
	Long string
	Name string
}

type AccumulatorData struct {
	OLat  string `json:"o_lat"`
	OLong string `json:"o_long"`
	FLat  string `json:"f_lat"`
	FLong string `json:"f_long"`
	Name  string `json:"name"`
}

type AccumulatorInfo struct {
	Data []AccumulatorData `json:"data"`
	common.EofData
}

func processData(station JoinerDataStation, accumulator map[string]sData, aq queue.Sender[AccumulatorInfo]) {
	if s := station.DataStation; s != nil {
		accumulator[getStationKey(station.DataStation.Code, s.Year)] = sData{Long: station.DataStation.Longitude, Lat: station.DataStation.Latitude, Name: station.DataStation.Name}
	} else if trip := station.DataTrip; trip != nil {
		t := *trip
		vToSend := make([]AccumulatorData, 0, len(t))
		for _, v := range *trip {
			if d, ok := obtainInfoToSend(accumulator, v); ok {
				vToSend = append(vToSend, d)
			}
		}
		aq.SendMessage(AccumulatorInfo{
			Data: vToSend,
		})
	}
}

func getStationKey(stationCode string, year int) string {
	return fmt.Sprintf("%s-%d", stationCode, year)
}

func obtainInfoToSend(accumulator map[string]sData, trip SendableDataTrip) (AccumulatorData, bool) {
	var ad AccumulatorData
	dOStation, ok := accumulator[getStationKey(trip.OStation, trip.Year)]
	dEStation, oke := accumulator[getStationKey(trip.EStation, trip.Year)]
	if !(ok && oke) {
		return ad, false
	}
	return AccumulatorData{
		OLat:  dOStation.Lat,
		OLong: dOStation.Long,
		FLat:  dEStation.Lat,
		FLong: dEStation.Long,
		Name:  dEStation.Name,
	}, true

}

type actionable struct {
	c  chan struct{}
	nc chan struct{}
}

func (a actionable) DoActionIfEOF() {
	a.nc <- <-a.c // continue the loop
}

func main() {
	amountCalc, err := strconv.Atoi(os.Getenv("calculators"))
	utils.FailOnError(err, "missing env value of calculator")
	workerStation, err := strconv.Atoi(os.Getenv("amountStationsWorkers"))
	utils.FailOnError(err, "missing env value of worker stations")
	workerTrips, err := strconv.Atoi(os.Getenv("amountTripsWorkers"))
	utils.FailOnError(err, "missing env value of worker trips")
	inputQueue, _ := queue.InitializeReceiver[JoinerDataStation]("montrealQueue", "rabbit", "", "", nil)
	inputQueueTrip, _ := queue.InitializeReceiver[JoinerDataStation]("montrealQueueTrip", "rabbit", "", "", nil)
	aq, _ := queue.InitializeSender[AccumulatorInfo]("calculatorMontreal", amountCalc, nil, "rabbit")
	sfe, _ := common.CreateConsumerEOF(nil, "montrealQueue", inputQueue, workerStation)
	tfe, _ := common.CreateConsumerEOF([]common.NextToNotify{{Name: "calculatorMontreal", Connection: aq}}, "montrealQueueTrip", inputQueueTrip, workerTrips)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer inputQueue.Close()
	defer aq.Close()
	defer inputQueueTrip.Close()
	tt := make(chan struct{}, 1)
	st := make(chan struct{}, 1)
	st <- struct{}{}
	// catch SIGETRM or SIGINTERRUPT
	acc := make(map[string]sData)
	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if data.EOF {
				sfe.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  st,
					nc: tt,
				})
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			d := <-st
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc, aq)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
			st <- d
		}
	}()
	go func() {
		for {
			data, msgId, err := inputQueueTrip.ReceiveMessage()
			if data.EOF {

				tfe.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  tt,
					nc: st,
				})
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			d := <-tt
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc, aq)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
			tt <- d
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}
