package main

import (
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

func processData(data AccumulatorData, m map[string]dStation, aq common.Queue[Accumulator, Accumulator]) {
	if data.EOF {
		v := make([]string, 0, len(m))
		for key, value := range m {
			if value.didItWentMoreThan(6) {
				v = append(v, key)
			}
		}
		_ = aq.SendMessage(Accumulator{Stations: v, Key: "random"}) // do graceful shutdown
		return
	}
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

func main() {
	inputQueue, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("preAccumulatorMontreal", "rabbit")
	outputQueue, _ := common.InitializeRabbitQueue[Accumulator, Accumulator]("accumulator", "rabbit")
	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		acc := make(map[string]dStation)
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc, outputQueue)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
