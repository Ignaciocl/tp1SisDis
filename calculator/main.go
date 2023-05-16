package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	log "github.com/sirupsen/logrus"
	lasPistasDeBlue "github.com/umahmood/haversine"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

type JoinerData struct {
	OLat  string `json:"o_lat"`
	OLong string `json:"o_long"`
	FLat  string `json:"f_lat"`
	FLong string `json:"f_long"`
	Name  string `json:"name"`
}

type JoinerInfo struct {
	Data []JoinerData `json:"data"`
	common.EofData
}

func (o JoinerData) calcDistance() (float64, error) {
	oLong, err := strconv.ParseFloat(o.OLong, 8)
	if err != nil {
		return 0, err
	}
	oLat, err := strconv.ParseFloat(o.OLat, 8)
	if err != nil {
		return 0, err
	}
	fLong, err := strconv.ParseFloat(o.FLong, 8)
	if err != nil {
		return 0, err
	}
	fLat, err := strconv.ParseFloat(o.FLat, 8)
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
}

type AccumulatorInfo struct {
	Data []AccumulatorData `json:"data"`
}

func getAccumulatorData(data []JoinerData) []AccumulatorData {
	v := make([]AccumulatorData, 0, len(data))
	for _, d := range data {
		distance, err := d.calcDistance()
		if err != nil {
			log.Errorf("could not resolve distance cal for: %v. skipping", d)
			continue
		}
		v = append(v, AccumulatorData{
			EndingStation: d.Name,
			Distance:      distance,
		})
	}
	return v
}
func main() {
	id := os.Getenv("id")
	inputQueue, _ := common.InitializeRabbitQueue[JoinerInfo, JoinerInfo]("calculatorMontreal", "rabbit", id, 0)
	aq, _ := common.InitializeRabbitQueue[AccumulatorInfo, AccumulatorInfo]("preAccumulatorMontreal", "rabbit", "", 0)
	me, _ := common.CreateConsumerEOF([]common.NextToNotify{{"preAccumulatorMontreal", aq}}, "calculatorMontreal", inputQueue, 1)
	defer inputQueue.Close()
	defer aq.Close()
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			common.FailOnError(err, "data is corrupted")
			if data.EOF {
				me.AnswerEofOk(data.IdempotencyKey, nil)
				continue
			}
			ad := getAccumulatorData(data.Data)
			aq.SendMessage(AccumulatorInfo{Data: ad})
		}
	}()
	<-oniChan
}
