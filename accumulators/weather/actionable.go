package main

import (
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
)

type actionable struct {
	acc *WeatherDuration
	q   queue.Sender[AccumulatorData]
	key string
}

func (a *actionable) DoActionIfEOF() {
	duration := 0.0
	if a.acc.Total != 0 {
		duration = float64(a.acc.Duration) / float64(a.acc.Total)
	}
	data := AccumulatorData{
		Dur: duration,
		Key: a.key,
		EofData: common.EofData{
			EOF:            false,
			IdempotencyKey: fmt.Sprintf("key:%s-duration:%f", a.key, duration),
		},
	}
	log.Infof("sending message: %+v", data)
	utils.LogError(a.q.SendMessage(data, ""), fmt.Sprintf("could not send message to accumulator, message to be sent: %+v", data))
	a.acc.Duration = 0
	a.acc.Total = 0
}
