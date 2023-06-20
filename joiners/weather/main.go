package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/fileManager"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
)

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

type actionable struct {
	c  chan struct{}
	nc chan struct{}
}

func (a actionable) DoActionIfEOF() {
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
	ns := make(chan struct{}, 1)
	weatherTurn <- struct{}{}
	fillMapWithData(acc, csvReader, actionable{
		c:  weatherTurn,
		nc: tripTurn,
	}, workerWeather)
	log.Info("data filled with info previously set")
	connection, _ := queue.InitializeConnectionRabbit(nil, "rabbit")
	inputQueue, _ := queue.InitializeReceiver[JoinerDataStation]("weatherQueue", "", "", "", connection)
	inputTrips, _ := queue.InitializeReceiver[JoinerDataStation]("weatherQueueTrip", "", "", "", connection)
	aq, _ := queue.InitializeSender[AccumulatorData]("accumulator", 0, connection, "")
	wqEOF, _ := common.CreateConsumerEOF(nil, "weatherQueue", inputQueue, workerWeather)
	tqEOF, _ := common.CreateConsumerEOF(nil, "weatherQueueTrip", inputTrips, workerTrips)
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
			data, id, err := inputQueue.ReceiveMessage()
			utils.LogError(csvReader.Write(data), "could not write info")
			if data.EOF {
				wqEOF.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  weatherTurn,
					nc: tripTurn,
				})
				inputQueue.AckMessage(id)
				continue
			}
			s := <-weatherTurn
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
			inputQueue.AckMessage(id)
			weatherTurn <- s
		}
	}() // For weather

	go func() {
		for {
			data, id, err := inputTrips.ReceiveMessage()
			if data.EOF {
				tqEOF.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  tripTurn,
					nc: ns,
				})
				inputTrips.AckMessage(id)
				continue
			}
			s := <-tripTurn
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
			inputTrips.AckMessage(id)
			tripTurn <- s
		}
	}() // For trip

	go func() {
		d := preAccumulatorData{
			DurGathered: 0,
			Amount:      0,
		}
		for i := 0; i < 3; i += 1 {
			<-ns
			v := weatherDuration{
				total:    0,
				duration: 0,
			}
			for _, value := range acc {
				v.addW(value)
			}
			d.DurGathered += v.duration
			d.Amount += v.total

			acc = make(map[string]weatherDuration, 0)
			if i != 2 {
				weatherTurn <- struct{}{}
			}
		}
		var l AccumulatorData
		if d.Amount == 0 {
			l = AccumulatorData{
				Dur: 0,
				Key: "random",
			}
		} else {
			l = AccumulatorData{
				Dur: float64(d.DurGathered) / float64(d.Amount),
				Key: "random",
			}
		}
		_ = aq.SendMessage(l, "")
		eof := AccumulatorData{EofData: common.EofData{
			EOF:            true,
			IdempotencyKey: "random",
		},
		}
		aq.SendMessage(eof, "")
		weatherTurn <- struct{}{}
	}()

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
			if maxAmountToContinue <= counter {
				a.DoActionIfEOF()
			}
			continue
		}
		processData(data, acc)
	}
}
