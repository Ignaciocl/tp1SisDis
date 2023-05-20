package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"strconv"
)

type ReceivableDataWeather struct {
	Date string `json:"date"`
}

type ReceivableDataTrip struct {
	Date     string `json:"date"`
	Duration int    `json:"duration"`
}

type JoinerDataStation struct {
	DataWeather *ReceivableDataWeather `json:"weatherData,omitempty"`
	DataTrip    *[]ReceivableDataTrip  `json:"tripData,omitempty"`
	Name        string                 `json:"name"`
	Key         string                 `json:"key"`
	common.EofData
}

type AccumulatorData struct {
	Dur float64 `json:"duration"`
	Key string  `json:"key"`
	common.EofData
}

type preAccumulatorData struct {
	DurGathered int
	Amount      int
}

type weatherDuration struct {
	total    int
	duration int
}

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
	common.FailOnError(err, "missing env value of worker stations")
	workerTrips, err := strconv.Atoi(os.Getenv("amountTripsWorkers"))
	common.FailOnError(err, "missing env value of worker trips")
	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("weatherQueue", "rabbit", "", 0)
	inputTrips, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("weatherQueueTrip", "rabbit", "", 0)
	aq, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("accumulator", "rabbit", "", 0)
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
	tripTurn := make(chan struct{}, 1)
	weatherTurn := make(chan struct{}, 1)
	ns := make(chan struct{}, 1)
	weatherTurn <- struct{}{}
	acc := make(map[string]weatherDuration)
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			if data.EOF {
				wqEOF.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  weatherTurn,
					nc: tripTurn,
				})
				continue
			}
			s := <-weatherTurn
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
			weatherTurn <- s
		}
	}() // For weather

	go func() {
		for {
			data, err := inputTrips.ReceiveMessage()
			if data.EOF {
				tqEOF.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  tripTurn,
					nc: ns,
				})
				continue
			}
			s := <-tripTurn
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
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
		_ = aq.SendMessage(l)
		eof := AccumulatorData{EofData: common.EofData{
			EOF:            true,
			IdempotencyKey: "random",
		},
		}
		aq.SendMessage(eof)
		weatherTurn <- struct{}{}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}
