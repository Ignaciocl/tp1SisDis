package main

import (
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

const serviceName = "joiner-weather"

func (sd *weatherDuration) add(duration int) {
	sd.total += 1
	sd.duration += duration
}

func (sd *weatherDuration) addW(w weatherDuration) {
	sd.total += w.total
	sd.duration += w.duration
}

func processData(data JoinerDataStation, accumulator map[string]weatherDuration) { // Check for city
	if weather := data.DataWeather; weather != nil {
		w := weatherDuration{
			total:    0,
			duration: 0,
		}
		accumulator[weather.Date] = w
	} else if trips := data.DataTrip; trips != nil {
		for _, trip := range *trips {
			if wd, ok := accumulator[trip.Date]; ok {
				wd.add(trip.Duration)
				accumulator[trip.Date] = wd
			}
		}
	}
}

func getTripsToSend(data JoinerDataStation, accumulator map[string]weatherDuration) WeatherDuration {
	toSend := WeatherDuration{
		Total:    0,
		Duration: 0,
	}
	if data.DataTrip == nil { // Border case that should never happen
		utils.LogError(errors.New("trip is empty"), "trips should not be empty")
		return toSend
	}
	for _, v := range *data.DataTrip {
		if _, ok := accumulator[v.Date]; !ok {
			continue
		}
		toSend.Total += 1
		toSend.Duration += v.Duration
	}
	return toSend
}

type actionable struct {
	c  chan struct{}
	nc chan struct{}
	m  map[string]weatherDuration
}

func (a actionable) DoActionIfEOF() {
	if a.m != nil {
		for k := range a.m {
			delete(a.m, k)
		}
	}
	a.nc <- <-a.c // continue the loop
}

func main() {
	workerWeather, err := strconv.Atoi(os.Getenv("amountWeatherWorkers"))
	utils.FailOnError(err, "missing env value of worker stations")
	workerTrips, err := strconv.Atoi(os.Getenv("amountTripsWorkers"))
	utils.FailOnError(err, "missing env value of worker trips")
	csvReader, err := fileManager.CreateCSVFileManager[JoinerDataStation](transformer{}, "ponemeElNombreLicha.csv")
	utils.FailOnError(err, "could not load csv file")
	acc := make(map[string]weatherDuration)
	tripTurn := make(chan struct{}, 1)
	weatherTurn := make(chan struct{}, 1)
	weatherTurn <- struct{}{}
	fillMapWithData(acc, csvReader, actionable{
		c:  weatherTurn,
		nc: tripTurn,
	}, workerWeather)
	log.Info("data filled with info previously set")
	id := os.Getenv("id")
	connection, _ := queue.InitializeConnectionRabbit(nil, "rabbit")
	inputQueue, _ := queue.InitializeReceiver[JoinerDataStation]("weatherQueue", "", id, "", connection)
	inputTrips, _ := queue.InitializeReceiver[JoinerDataStation]("weatherQueueTrip", "", id, "", connection)
	aq, _ := queue.InitializeSender[ToAccWeather]("weatherAccumulator", 0, connection, "")
	wqEOF, _ := common.CreateConsumerEOF(nil, "weatherQueue", inputQueue, workerWeather)
	tqEOF, _ := common.CreateConsumerEOF([]common.NextToNotify{{"weatherAccumulator", aq}}, "weatherQueueTrip", inputTrips, workerTrips)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer wqEOF.Close()
	defer tqEOF.Close()
	defer inputQueue.Close()
	defer aq.Close()
	defer inputTrips.Close()
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
				wqEOF.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  weatherTurn,
					nc: tripTurn,
				})
				inputQueue.AckMessage(msgId)
				continue
			}
			s := <-weatherTurn
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
			inputQueue.AckMessage(msgId)
			weatherTurn <- s
		}
	}() // For weather

	go func() {
		for {
			data, msgId, err := inputTrips.ReceiveMessage()
			if data.EOF {
				if !strings.HasSuffix(data.IdempotencyKey, id) {
					log.Infof("eof received from another client: %s, not propagating", data.IdempotencyKey)
					utils.LogError(inputTrips.AckMessage(msgId), "could not acked message")
					continue
				}
				tqEOF.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  tripTurn,
					nc: weatherTurn,
					m:  acc,
				})
				inputTrips.AckMessage(msgId)
				continue
			}
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			s := <-tripTurn
			t := getTripsToSend(data, acc)
			utils.LogError(aq.SendMessage(ToAccWeather{
				Data: t,
				EofData: common.EofData{
					EOF:            false,
					IdempotencyKey: data.IdempotencyKey,
				},
				Key: id,
			}, id), "could not send message to accumulator")
			utils.LogError(inputTrips.AckMessage(msgId), "could not acked message")
			tripTurn <- s
		}
	}() // For trip

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

func fillMapWithData(acc map[string]weatherDuration, manager fileManager.Manager[JoinerDataStation], a actionable, maxAmountToContinue int) {
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
