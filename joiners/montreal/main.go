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
	"strings"
)

const (
	storageFilename = "montreal_joiner.csv"
	serviceName     = "joiner-montreal"
)

type AccumulatorInfo struct {
	Data []AccumulatorData `json:"data"`
	common.EofData
}

func processData(station JoinerDataStation, accumulator map[string]sData) {
	if s := station.DataStation; s != nil {
		accumulator[getStationKey(station.DataStation.Code, s.Year)] = sData{Long: station.DataStation.Longitude, Lat: station.DataStation.Latitude, Name: station.DataStation.Name}
	}
}

func processTripData(trip *[]SendableDataTrip, accumulator map[string]sData, aq queue.Sender[AccumulatorInfo], ik string, id string) {
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
		EofData: common.EofData{
			IdempotencyKey: ik,
		},
	}, id)
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

type clearable interface {
	Clear()
}

type actionable struct {
	c  chan struct{}
	nc chan struct{}
	cl clearable
}

func (a actionable) DoActionIfEOF() {
	a.nc <- <-a.c // continue the loop
	if a.cl != nil {
		a.cl.Clear()
	}
}

func main() {
	id := os.Getenv("id")
	if id == "" {
		panic("missing Montreal Joiner ID")
	}
	amountCalc, err := strconv.Atoi(os.Getenv("calculators"))
	utils.FailOnError(err, "missing env value of calculator")
	workerStation, err := strconv.Atoi(os.Getenv("amountStationsWorkers"))
	utils.FailOnError(err, "missing env value of worker stations")
	workerTrips, err := strconv.Atoi(os.Getenv("amountTripsWorkers"))
	utils.FailOnError(err, "missing env value of worker trips")
	acc := make(map[string]sData)
	csvReader, err := fileManager.CreateCSVFileManager[JoinerDataStation](transformer{}, storageFilename)
	utils.FailOnError(err, "could not load csv file")
	tt := make(chan struct{}, 1)
	st := make(chan struct{}, 1)
	st <- struct{}{}
	fillMapWithData(acc, csvReader, actionable{
		c:  st,
		nc: tt,
	}, workerStation)
	log.Info("data filled with info previously set")
	inputQueue, _ := queue.InitializeReceiver[JoinerDataStation]("montrealQueue", "rabbit", id, "", nil)
	inputQueueTrip, _ := queue.InitializeReceiver[JoinerDataStation]("montrealQueueTrip", "rabbit", id, "", nil)
	aq, _ := queue.InitializeSender[AccumulatorInfo]("calculatorMontreal", amountCalc, nil, "rabbit")
	sfe, _ := common.CreateConsumerEOF(nil, "montrealQueue", inputQueue, workerStation)
	tfe, _ := common.CreateConsumerEOF([]common.NextToNotify{{Name: "calculatorMontreal", Connection: aq}}, "montrealQueueTrip", inputQueueTrip, workerTrips)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer inputQueue.Close()
	defer aq.Close()
	defer inputQueueTrip.Close()
	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			utils.LogError(csvReader.Write(data), "could not write info")
			if data.EOF {
				if !strings.HasSuffix(data.IdempotencyKey, id) {
					log.Infof("eof received from another client: %s, not propagating", data.IdempotencyKey)
					utils.LogError(inputQueue.AckMessage(msgId), "could not acked message")
					continue
				}
				sfe.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  st,
					nc: tt,
				})
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			d := <-st
			processData(data, acc)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
			st <- d
		}
	}()
	go func() {
		for {
			data, msgId, err := inputQueueTrip.ReceiveMessage()
			if data.EOF {
				if !strings.HasSuffix(data.IdempotencyKey, id) {
					log.Infof("eof received from another client: %s, not propagating", data.IdempotencyKey)
					utils.LogError(inputQueueTrip.AckMessage(msgId), "could not acked message")
					continue
				}
				tfe.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  tt,
					nc: st,
					cl: csvReader,
				})
				utils.LogError(inputQueueTrip.AckMessage(msgId), "failed while trying ack")
				continue
			}
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			d := <-tt
			processTripData(data.DataTrip, acc, aq, data.IdempotencyKey, id)
			utils.LogError(inputQueueTrip.AckMessage(msgId), "failed while trying ack")
			tt <- d
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName + id)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "health check error")
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

func fillMapWithData(acc map[string]sData, manager fileManager.Manager[JoinerDataStation], a actionable, maxAmountToContinue int) {
	counter := 0
	for {
		data, err := manager.ReadLine()
		if err != nil && errors.Is(err, io.EOF) {
			break
		}
		utils.FailOnError(err, "could not parse line from file")
		if data.EOF {
			counter += 1
			continue
		}
		processData(data, acc)
	}
	if counter%maxAmountToContinue == 0 && counter > 0 {
		a.DoActionIfEOF()
	}
}
