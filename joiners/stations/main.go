package main

import (
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/fileManager"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
	"strings"
)

const serviceName = "joiner-stations"

func (s *stationAlive) shouldBeConsidered() bool {
	return s.wasAliveOn16 && s.wasAliveOn17
}

func (s *stationAlive) setAliveForYear(year int) {
	if year == 2016 {
		s.wasAliveOn16 = true
	} else if year == 2017 {
		s.wasAliveOn17 = true
	}
}

type mapHolder struct {
	m             map[string]stationData
	stationToYear map[string]stationAlive
}

func getStationData(key string, city string, year int, accumulator map[string]stationData) (stationData, error) {
	data, ok := accumulator[getStationKey(key, year, city)]
	var err error
	if !(ok) {
		data = stationData{
			name: "",
		}
		err = errors.New("data does not exist")
	}
	return data, err
}

func processData(data JoinerDataStation, w *mapHolder) {
	accumulator := w.m
	alive := w.stationToYear
	city := data.City
	if station := data.DataStation; station != nil {
		sData, _ := getStationData(station.Code, city, station.Year, accumulator)
		sData.name = station.Name
		accumulator[getStationKey(station.Code, station.Year, city)] = sData
		var sa stationAlive
		if d, ok := alive[station.Name]; ok {
			sa = d
		} else {
			sa = stationAlive{
				wasAliveOn17: false,
				wasAliveOn16: false,
			}
		}
		sa.setAliveForYear(station.Year)
		alive[station.Name] = sa

	}
}

func processDataTrips(data JoinerDataStation, w *mapHolder, aq queue.Sender[PreAccumulatorData], id string) {
	accumulator := w.m
	alive := w.stationToYear
	if trips := data.DataTrip; trips != nil {
		v := make([]senderDataStation, 0, len(*trips))
		for _, trip := range *trips {
			dStation, err := getStationData(trip.Station, data.City, trip.Year, accumulator)
			if err != nil {
				continue
			}
			if d, ok := alive[dStation.name]; !(ok && d.shouldBeConsidered()) {
				continue
			}
			v = append(v, senderDataStation{
				Name: dStation.name,
				Year: trip.Year,
			})
		}
		aq.SendMessage(PreAccumulatorData{
			Data:    v,
			Key:     data.IdempotencyKey,
			EofData: common.EofData{IdempotencyKey: data.IdempotencyKey},
		}, id)
	}
}

func getStationKey(stationCode string, year int, city string) string {
	return fmt.Sprintf("%s-%s-%d", stationCode, city, year)
}

type actionable struct {
	c  chan struct{}
	nc chan struct{}
}

func (a actionable) DoActionIfEOF() {
	a.nc <- <-a.c // continue the loop
}

func main() {
	workerStation, err := strconv.Atoi(os.Getenv("amountStationsWorkers"))
	utils.FailOnError(err, "missing env value of worker stations")
	workerTrips, err := strconv.Atoi(os.Getenv("amountTripsWorkers"))
	utils.FailOnError(err, "missing env value of worker trips")
	csvReader, err := fileManager.CreateCSVFileManager[JoinerDataStation](transformer{}, "ponemeElNombreLicha.csv")
	utils.FailOnError(err, "could not load csv file")
	acc := map[string]stationData{}
	tt := make(chan struct{}, 1)
	st := make(chan struct{}, 1)
	st <- struct{}{}
	aliveStations := map[string]stationAlive{}
	w := mapHolder{m: acc, stationToYear: aliveStations}
	fillMapWithData(&w, csvReader, actionable{
		c:  st,
		nc: tt,
	}, workerStation)
	log.Info("data filled with info previously set")
	id := os.Getenv("id")
	inputQueue, _ := queue.InitializeReceiver[JoinerDataStation]("stationsQueue", "rabbit", id, "", nil)
	inputQueueTrip, _ := queue.InitializeReceiver[JoinerDataStation]("stationsQueueTrip", "rabbit", id, "", nil)
	aq, _ := queue.InitializeSender[PreAccumulatorData]("preAccumulatorSt", 0, nil, "rabbit")
	sfe, _ := common.CreateConsumerEOF(nil, "stationsQueue", inputQueue, workerStation)
	tfe, _ := common.CreateConsumerEOF([]common.NextToNotify{{"preAccumulatorSt", aq}}, "stationsQueueTrip", inputQueueTrip, workerTrips)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer sfe.Close()
	defer tfe.Close()
	defer inputQueue.Close()
	defer aq.Close()
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
			p := <-st
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, &w)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
			st <- p
		}
	}()
	go func() {
		for {
			data, msgId, err := inputQueueTrip.ReceiveMessage()
			if data.EOF {
				log.Printf("joiner station trip eof received")
				if !strings.HasSuffix(data.IdempotencyKey, id) {
					log.Infof("eof received from another client: %s, not propagating", data.IdempotencyKey)
					utils.LogError(inputQueueTrip.AckMessage(msgId), "could not acked message")
					continue
				}
				tfe.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  tt,
					nc: st,
				})
				utils.LogError(inputQueueTrip.AckMessage(msgId), "failed while trying ack")
				continue
			}
			p := <-tt
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			processDataTrips(data, &w, aq, id)
			utils.LogError(inputQueueTrip.AckMessage(msgId), "failed while trying ack")
			tt <- p
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName)
	go func() {
		err := healthCheckerReplier.Run()
		log.Errorf("healtchecker error: %v", err)
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

func fillMapWithData(acc *mapHolder, manager fileManager.Manager[JoinerDataStation], a actionable, maxAmountToContinue int) {
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
