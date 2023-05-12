package main

import (
	"encoding/json"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type dStation struct {
	counter         float64
	distanceCounted float64
}

func (d *dStation) add(distance float64) {
	d.distanceCounted += distance
	d.counter += 1
}

func (d *dStation) didItWentMoreThan(distanceAvg float64) bool {
	return (d.distanceCounted / d.counter) > distanceAvg
}

type AccumulatorData struct {
	EndingStation string  `json:"ending_station"`
	Distance      float64 `json:"distance"`
	EOF           bool    `json:"eof"`
}

type Accumulator struct {
	Key      string   `json:"key"`
	Stations []string `json:"stations"`
}

func processData(data AccumulatorData, m map[string]dStation) {
	station, ok := m[data.EndingStation]
	if !ok {
		station = dStation{
			counter:         0,
			distanceCounted: 0,
		}
	}
	station.add(data.Distance)
	m[data.EndingStation] = station
}

type checker struct {
	blocker     chan struct{}
	eofReceived chan struct{}
}

func (c checker) IsStillUsingNecessaryDataForFile(file string, city string) bool {
	d := <-c.blocker
	c.eofReceived <- d
	log.Printf("eof is received")
	c.blocker <- d
	return true
}

func main() {
	inputQueue, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("preAccumulatorMontreal", "rabbit")
	outputQueue, _ := common.InitializeRabbitQueue[Accumulator, Accumulator]("accumulator", "rabbit")
	wfe, _ := common.CreateConsumerEOF("rabbit", "accumulatorMontreal")
	defer inputQueue.Close()
	defer outputQueue.Close()
	defer wfe.Close()
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	blocker := make(chan struct{}, 1)
	eofReceived := make(chan struct{}, 1)
	blocker <- struct{}{}
	c := checker{eofReceived: eofReceived, blocker: blocker}
	finished := false
	go func() {
		wfe.AnswerEofOk(c)
	}()
	acc := make(map[string]dStation)
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			d := <-blocker
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			p, _ := json.Marshal(data)
			log.Printf("DATA IN MONTREAL 2 RECEIVED IS: %s, passed: %v\n", string(p), finished)
			processData(data, acc)
			blocker <- d
		}
	}()
	go func() {
		r := 0
		for {
			<-eofReceived
			r += 1
			if r > 1 {
				break
			}
		}
		r = 0
		for {
			d := <-blocker
			if inputQueue.IsEmpty() {
				break
			}
			blocker <- d
		}
		v := make([]string, 0, len(acc))
		for key, value := range acc {
			if value.didItWentMoreThan(5) {
				v = append(v, key)
			}
		}
		log.Printf("stations to check are %v and passed: %v\n", acc, v)
		_ = outputQueue.SendMessage(Accumulator{Stations: v, Key: "random"}) // do graceful shutdown
		finished = true
		blocker <- struct{}{}
		return
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
