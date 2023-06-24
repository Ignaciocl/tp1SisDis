package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	lasPistasDeBlue "github.com/umahmood/haversine"
	"os"
	"strconv"
)

const serviceName = "calculator"

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
	inputQueue, _ := queue.InitializeReceiver[JoinerInfo]("calculatorMontreal", "rabbit", id, "", nil)
	aq, _ := queue.InitializeSender[AccumulatorInfo]("preAccumulatorMontreal", 0, nil, "rabbit")
	me, _ := common.CreateConsumerEOF([]common.NextToNotify{{"preAccumulatorMontreal", aq}}, "calculatorMontreal", inputQueue, 1)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer inputQueue.Close()
	defer aq.Close()
	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			utils.FailOnError(err, "data is corrupted")
			if data.EOF {
				me.AnswerEofOk(data.IdempotencyKey, nil)
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			ad := getAccumulatorData(data.Data)
			aq.SendMessage(AccumulatorInfo{Data: ad}, "")
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName + id)
	go func() {
		err := healthCheckerReplier.Run()
		log.Errorf("healtchecker error: %v", err)
	}()

	common.WaitForSigterm(grace)
}
