package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strconv"
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
}

type AccumulatorInfo struct {
	Data []AccumulatorData `json:"data"`
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
	amountCalc, err := strconv.Atoi(os.Getenv("calculators"))
	common.FailOnError(err, "missing env value of calculator")
	inputQueue, _ := common.InitializeRabbitQueue[AccumulatorInfo, AccumulatorInfo]("preAccumulatorMontreal", "rabbit", "", 0)
	outputQueue, _ := common.InitializeRabbitQueue[Accumulator, Accumulator]("accumulator", "rabbit", "", 0)
	me, _ := common.CreateConsumerEOF(nil, "preAccumulatorMontreal", inputQueue, amountCalc)
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
			dataInfo, err := inputQueue.ReceiveMessage()
			if dataInfo.EOF {

				me.AnswerEofOk(dataInfo.IdempotencyKey, actionable{
					nc:   eof,
					data: dataInfo.EofData,
				})
				continue
			}

			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			for _, d := range dataInfo.Data {
				processData(d, acc)
			}
		}
	}()
	go func() {
		d := <-eof
		d.IdempotencyKey = "random"
		va := make([]dStation, 0)
		v := make([]string, 0, len(acc))
		for key, value := range acc {
			if value.didItWentMoreThan(6) {
				v = append(v, key)
				va = append(va, value)
			}
		}
		_ = outputQueue.SendMessage(Accumulator{Stations: v, Key: "random"}) // do graceful shutdown
		_ = outputQueue.SendMessage(Accumulator{EofData: d})
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
}
