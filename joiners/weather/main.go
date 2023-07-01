package main

import (
	"encoding/json"
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
	serviceName     = "joiner-weather"
	storageFilename = "weather_joiner.csv"
)

func (sd *weatherDuration) add(duration int) {
	sd.total += 1
	sd.duration += duration
}

func (sd *weatherDuration) addW(w weatherDuration) {
	sd.total += w.total
	sd.duration += w.duration
}

func getKey(date, city string) string {
	return fmt.Sprintf("%s-%s", date, city)
}

func processData(data JoinerDataStation, accumulator map[string]weatherDuration, city string) { // Check for city
	if weather := data.DataWeather; weather != nil {
		w := weatherDuration{
			total:    0,
			duration: 0,
		}
		accumulator[getKey(weather.Date, city)] = w
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
		if _, ok := accumulator[getKey(v.Date, data.City)]; !ok {
			continue
		}
		toSend.Total += 1
		toSend.Duration += v.Duration
	}
	return toSend
}

type clearable interface {
	Clear()
}

type actionable struct {
	c       chan struct{}
	nc      chan struct{}
	m       map[string]weatherDuration
	cl      clearable
	counter *int
}

func (a actionable) DoActionIfEOF() {
	*a.counter = *a.counter - 1
	if *a.counter <= 0 {
		a.nc <- <-a.c // continue the loop
		*a.counter = 3
		if a.cl != nil {
			a.cl.Clear()
		}
	}
}

type t struct {
}

const Sep = "|pepe|"

func (t t) ToWritable(data *JoinerDataStation) []byte {
	returnable, _ := json.Marshal(data)
	return returnable
}

func (t t) FromWritable(d []byte) *JoinerDataStation {
	data := strings.Split(string(d), Sep)[0]
	var r JoinerDataStation
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		utils.LogError(err, "could not unmarshal from db")
	}
	return &r
}

func main() {
	id := os.Getenv("id")
	if id == "" {
		panic("missing weather joiner ID")
	}

	workerWeather, err := strconv.Atoi(os.Getenv("amountWeatherWorkers"))
	utils.FailOnError(err, "missing env value of worker stations")
	workerTrips, err := strconv.Atoi(os.Getenv("amountTripsWorkers"))
	utils.FailOnError(err, "missing env value of worker trips")
	csvReader, err := fileManager.CreateCSVFileManager[JoinerDataStation](transformer{}, storageFilename)
	utils.FailOnError(err, "could not load csv file")
	acc := make(map[string]weatherDuration)
	tripTurn := make(chan struct{}, 1)
	weatherTurn := make(chan struct{}, 1)
	weatherTurn <- struct{}{}
	log.Info("data filled with info previously set")
	connection, _ := queue.InitializeConnectionRabbit(nil, "rabbit")
	inputQueue, _ := queue.InitializeReceiver[JoinerDataStation]("weatherQueue", "", id, "", connection)
	inputTrips, _ := queue.InitializeReceiver[JoinerDataStation]("weatherQueueTrip", "", id, "", connection)
	aq, _ := queue.InitializeSender[ToAccWeather]("weatherAccumulator", 0, connection, "")
	wqEOF, _ := common.CreateConsumerEOF(nil, "weatherQueue", inputQueue, workerWeather)
	tqEOF, _ := common.CreateConsumerEOF([]common.NextToNotify{{"weatherAccumulator", aq}}, "weatherQueueTrip", inputTrips, workerTrips)
	grace, _ := common.CreateGracefulManager("rabbit")
	other, _ := fileManager.CreateDB[*JoinerDataStation](t{}, "pepe.csv", 3000, Sep)
	eofDb, err := fileManager.CreateDB[*eofData](t2{}, eofStorageFilename, 300, Sep)
	utils.FailOnError(err, "could not create eof")
	counter := 3
	fillMapWithData(acc, csvReader, actionable{
		c:       weatherTurn,
		nc:      tripTurn,
		counter: &counter,
	}, wqEOF, eofDb, workerWeather, tqEOF)
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
			utils.LogError(other.Write(&data), "could not write into db")
			if data.EOF {
				if !strings.HasSuffix(data.IdempotencyKey, id) {
					log.Infof("eof received from another client: %s, not propagating", data.IdempotencyKey)
					utils.LogError(inputQueue.AckMessage(msgId), "could not acked message")
					continue
				}
				wqEOF.AnswerEofOk(data.IdempotencyKey, actionable{
					c:       weatherTurn,
					nc:      tripTurn,
					counter: &counter,
				})
				inputQueue.AckMessage(msgId)
				continue
			}
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc, data.City)
			inputQueue.AckMessage(msgId)
		}
	}() // For weather

	go func() {
		for {
			data, msgId, err := inputTrips.ReceiveMessage()
			utils.LogError(other.Write(&data), "could not write into db")
			if data.EOF {
				if !strings.HasSuffix(data.IdempotencyKey, id) {
					log.Infof("eof received from another client: %s, not propagating", data.IdempotencyKey)
					utils.LogError(inputTrips.AckMessage(msgId), "could not acked message")
					continue
				}
				tqEOF.AnswerEofOk(data.IdempotencyKey, actionable{
					c:       tripTurn,
					nc:      weatherTurn,
					m:       acc,
					counter: &counter,
					//cl: csvReader,
				})
				inputTrips.AckMessage(msgId)
				utils.LogError(eofDb.Write(&eofData{
					IdempotencyKey: data.IdempotencyKey,
					Id:             0,
				}), "could not write eof")
				continue
			}
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			s := <-tripTurn
			log.Infof("acc is: %+v", acc)
			t := getTripsToSend(data, acc)
			if t.Total > 0 {
				utils.LogError(aq.SendMessage(ToAccWeather{
					Data: t,
					EofData: common.EofData{
						EOF:            false,
						IdempotencyKey: data.IdempotencyKey,
					},
					ClientID: id,
				}, id), "could not send message to accumulator")
			}
			utils.LogError(inputTrips.AckMessage(msgId), "could not acked message")
			tripTurn <- s
		}
	}() // For trip

	healthCheckReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName + id)
	go func() {
		err := healthCheckReplier.Run()
		utils.FailOnError(err, "health check error")
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

func fillMapWithData(acc map[string]weatherDuration, manager fileManager.Manager[JoinerDataStation], a actionable, db common.WaitForEof, eofDb fileManager.Manager[*eofData], maxAmountToContinue int, tfe common.WaitForEof) {
	for {
		data, err := manager.ReadLine()
		if err != nil && errors.Is(err, io.EOF) {
			break
		}
		utils.FailOnError(err, "could not parse line from file")
		if data.EOF {
			db.AnswerEofOk(data.IdempotencyKey, a)
			log.Infof("was eof")
			continue
		}
		processData(data, acc, data.City)

	}
	log.Infof("data revived is: %+v", acc)
	d := make(map[string]int, 0)
	for {
		data, err := eofDb.ReadLine()
		if err != nil && errors.Is(err, io.EOF) {
			break
		}
		utils.FailOnError(err, "could not parse line from file")
		i := d[data.IdempotencyKey]
		d[data.IdempotencyKey] = i + 1
	}
	for k, v := range d {
		if v == maxAmountToContinue {
			*a.counter -= 1
		} else {
			for i := 0; i < v; i++ {
				tfe.AnswerEofOk(k, a)
			}
		}
	}
	if *a.counter == 0 {
		a.c <- <-a.nc
		*a.counter = 3
	}
}
