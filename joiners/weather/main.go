package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
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
	DataTrip    *ReceivableDataTrip    `json:"tripData,omitempty"`
	Name        string                 `json:"name"`
	Key         string                 `json:"key"`
	EOF         bool                   `json:"EOF"`
}

type AccumulatorData struct {
	Dur float64 `json:"durationPrect"`
	Key string  `json:"key"`
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
	} else if trip := data.DataTrip; trip != nil {
		if wd, ok := accumulator[trip.Date]; ok {
			wd.add(trip.Duration)
			accumulator[trip.Date] = wd
		} else {
			log.Printf("date of trip not found for trip %v and map of values of: %v\n", trip, accumulator)
		}
	}
}

type checker struct {
	data           map[string]string
	blocker        chan struct{}
	tripBlocker    chan struct{}
	weatherBlocker chan struct{}
	wt             chan struct{}
	tt             chan struct{}
}

func (c checker) IsStillUsingNecessaryDataForFile(file string, city string) bool {
	d := <-c.blocker
	if file == "trips" {
		c.tripBlocker <- d
		c.wt <- <-c.tt
	} else if file == "weather" {
		c.weatherBlocker <- d
		c.tt <- <-c.wt
	}
	c.blocker <- d
	return true
}

func main() {
	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("weatherQueue", "rabbit")
	inputTrips, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("weatherQueueTrip", "rabbit")
	aq, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("accumulator", "rabbit")
	wfe, _ := common.CreateConsumerEOF("rabbit", "joinerWeather")
	oniChan := make(chan os.Signal, 1)
	defer inputQueue.Close()
	defer aq.Close()
	defer wfe.Close()
	defer inputTrips.Close()
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	eofCheck := map[string]string{}
	eofCheck["city"] = ""
	blocker := make(chan struct{}, 1)
	tb := make(chan struct{}, 1)
	wb := make(chan struct{}, 1)
	tripTurn := make(chan struct{}, 0)
	weatherTurn := make(chan struct{}, 1)
	blocker <- struct{}{}
	c := checker{data: eofCheck, blocker: blocker, weatherBlocker: wb, tripBlocker: tb, wt: weatherTurn, tt: tripTurn}
	go func() {
		wfe.AnswerEofOk(c)
	}()
	acc := make(map[string]weatherDuration)
	go func() {
		for {
			s := <-weatherTurn
			data, err := inputQueue.ReceiveMessage()
			d := <-blocker
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
			blocker <- d
			weatherTurn <- s
		}
	}() // For weather

	go func() {
		for {
			s := <-tripTurn
			data, err := inputTrips.ReceiveMessage()
			d := <-blocker
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
			blocker <- d
			tripTurn <- s
		}
	}() // For weather

	go func() {
		d := preAccumulatorData{
			DurGathered: 0,
			Amount:      0,
		}
		for i := 0; i < 3; i += 1 {
			<-wb
			<-tb
			<-blocker
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
			<-blocker
		}
		l := AccumulatorData{
			Dur: float64(d.DurGathered) / float64(d.Amount),
			Key: "random",
		}
		_ = aq.SendMessage(l)
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
