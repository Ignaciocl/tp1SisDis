package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
)

type ReceivableDataStation struct {
	Name string `json:"name"`
	Year int    `json:"year"`
}

type JoinerDataStation struct {
	DataStation []ReceivableDataStation `json:"data,omitempty"`
	common.EofData
}

type AccumulatorData struct {
	AvgStations []string `json:"avg_stations"`
	Key         string   `json:"key"`
	common.EofData
}

type stationData struct {
	sweetSixteen int
	sadSeventeen int
	name         string
}

func (sd *stationData) wasDouble() bool {
	return sd.sadSeventeen > 2*sd.sweetSixteen
}

func (sd *stationData) addYear(year int) {
	if year == 2016 {
		sd.sweetSixteen += 1
	} else if year == 2017 {
		sd.sadSeventeen += 1
	}
}

func processData(data JoinerDataStation, acc map[string]stationData) {
	if data.DataStation == nil {
		return
	}
	for _, ds := range data.DataStation {
		if d, ok := acc[ds.Name]; ok {
			d.addYear(ds.Year)
			acc[ds.Name] = d
		} else {
			nd := stationData{
				sweetSixteen: 0,
				sadSeventeen: 0,
				name:         ds.Name,
			}
			nd.addYear(ds.Year)
			acc[ds.Name] = nd
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
	inputQueue, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("preAccumulatorSt", "rabbit", "", 0)
	aq, _ := common.InitializeRabbitQueue[AccumulatorData, AccumulatorData]("accumulator", "rabbit", "", 0)
	sfe, _ := common.CreateConsumerEOF(nil, "preAccumulatorSt", inputQueue, 1)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer sfe.Close()
	defer inputQueue.Close()
	defer aq.Close()
	ns := make(chan struct{}, 1)
	st := make(chan struct{}, 1)
	st <- struct{}{}
	acc := map[string]stationData{}
	go func() {
		for {
			data, err := inputQueue.ReceiveMessage()
			if data.EOF {

				sfe.AnswerEofOk(data.IdempotencyKey, actionable{
					c:  st,
					nc: ns,
				})
				continue
			}
			p := <-st
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			processData(data, acc)
			st <- p
		}
	}()
	go func() {
		savedData := make(map[string][]int, 0)
		for i := 0; i < 3; i += 1 {
			<-ns
			for _, value := range acc {
				if value.wasDouble() && value.name != "" {
					savedData[value.name] = []int{value.sweetSixteen, value.sadSeventeen}
				}
			}

			if i != 2 {
				st <- struct{}{}
			}
		}
		v := make([]string, 0, len(savedData))
		for key, _ := range savedData {
			v = append(v, key)
		}
		l := AccumulatorData{
			AvgStations: v,
			Key:         "random",
		}

		_ = aq.SendMessage(l)
		eof := AccumulatorData{EofData: common.EofData{
			EOF:            true,
			IdempotencyKey: "random",
		},
		}
		aq.SendMessage(eof)
		st <- struct{}{}
	}()

	common.WaitForSigterm(grace)
}
