package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
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

func processData(station JoinerDataStation, accumulator map[string]sData, aq common.Queue[AccumulatorInfo, AccumulatorInfo]) {
	if s := station.DataStation; s != nil {
		accumulator[station.DataStation.Code] = sData{Long: station.DataStation.Longitude, Lat: station.DataStation.Latitude, Name: station.DataStation.Name}
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

func obtainInfoToSend(accumulator map[string]sData, trip SendableDataTrip) (AccumulatorData, bool) {
	var ad AccumulatorData
	dOStation, ok := accumulator[trip.OStation]
	dEStation, oke := accumulator[trip.EStation]
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
	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("montrealQueue", "rabbit", "", 0)
	inputQueueTrip, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("montrealQueueTrip", "rabbit", "", 0)
	aq, _ := common.InitializeRabbitQueue[AccumulatorInfo, AccumulatorInfo]("calculatorMontreal", "rabbit", "", 3)
	sfe, _ := common.CreateConsumerEOF(nil, "montrealQueue", inputQueue, 3)
	tfe, _ := common.CreateConsumerEOF([]common.NextToNotify{{Name: "calculatorMontreal", Connection: aq}}, "montrealQueueTrip", inputQueueTrip, 3)
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
				log.Infof("data received is: %v", data)
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
