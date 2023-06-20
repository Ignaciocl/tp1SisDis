package main

import (
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/fileManager"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
)

const serviceName = "joiner-montreal"

type AccumulatorInfo struct {
	Data []AccumulatorData `json:"data"`
	common.EofData
}

func processData(station JoinerDataStation, accumulator map[string]sData) {
	if s := station.DataStation; s != nil {
		accumulator[getStationKey(station.DataStation.Code, s.Year)] = sData{Long: station.DataStation.Longitude, Lat: station.DataStation.Latitude, Name: station.DataStation.Name}
	}
}

func processTripData(trip *[]SendableDataTrip, accumulator map[string]sData, aq queue.Sender[AccumulatorInfo]) {
	if trip == nil {
		log.Error("trip is nil, no processing is made but something must be checked")
		return
	}
	t := *trip
	vToSend := make([]AccumulatorData, 0, len(t))
	for _, v := range *trip {
		if d, ok := obtainInfoToSend(accumulator, v); ok {
			vToSend = append(vToSend, d)
		}
	}
	aq.SendMessage(AccumulatorInfo{
		Data: vToSend,
	}, "")
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
	acc := make(map[string]sData)
	csvReader, err := fileManager.CreateCSVFileManager[JoinerDataStation](transformer{}, "ponemeElNombreLicha.csv")
	utils.FailOnError(err, "could not load csv file")
	fillMapWithData(acc, csvReader)
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
			processData(data, acc)
			utils.LogError(csvReader.Write(data), "could not write info")
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
				utils.LogError(inputQueueTrip.AckMessage(msgId), "failed while trying ack")
				continue
			}
			d := <-tt
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			processTripData(data.DataTrip, acc, aq)
			utils.LogError(inputQueueTrip.AckMessage(msgId), "failed while trying ack")
			tt <- d
		}
	}()

	healthCheckHandler := commonHealthcheck.InitHealthChecker(serviceName)
	go func() {
		err := healthCheckHandler.Run()
		log.Errorf("healtchecker error: %v", err)
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

func fillMapWithData(acc map[string]sData, manager fileManager.Manager[JoinerDataStation]) {
	for {
		data, err := manager.ReadLine()
		if err != nil && errors.Is(err, io.EOF) {
			break
		}
		utils.FailOnError(err, "could not parse line from file")
		processData(data, acc)
	}
}
