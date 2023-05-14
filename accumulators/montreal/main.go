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
	common.EofData
}

type Accumulator struct {
	Key      string   `json:"key"`
	Stations []string `json:"stations"`
	common.EofData
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

type actionable struct {
	nc   chan common.EofData
	data common.EofData
}

func (a actionable) DoActionIfEOF() {
	a.nc <- a.data // continue the loop
}

func main() {
	inputQueue, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("preAccumulatorMontreal", "rabbit", "", 0)
	outputQueue, _ := common.InitializeRabbitQueue[Accumulator, Accumulator]("accumulator", "rabbit", "", 0)
	me, _ := common.CreateConsumerEOF(nil, "preAccumulatorMontrealEOF", inputQueue, 1)
	defer me.Close()
	defer inputQueue.Close()
	defer outputQueue.Close()
	oniChan := make(chan os.Signal, 1)
	eof := make(chan common.EofData, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	acc := make(map[string]dStation)
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			if data.EOF {

				me.AnswerEofOk(data.IdempotencyKey, actionable{
					nc:   eof,
					data: data.EofData,
				})
				continue
			}

			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
		}
	}()
	go func() {
		d := <-eof
		d.IdempotencyKey = "random"
		v := make([]string, 0, len(acc))
		for key, value := range acc {
			if value.didItWentMoreThan(6) {
				v = append(v, key)
			}
		}
		_ = outputQueue.SendMessage(Accumulator{Stations: v, Key: "random"}) // do graceful shutdown
		_ = outputQueue.SendMessage(Accumulator{EofData: d})
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
