package main

import (
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
)

type actionable struct {
	acc map[string]dStation
	q   queue.Sender[Accumulator]
	id  string
}

func (a actionable) DoActionIfEOF() {
	v := make([]string, 0, len(a.acc))
	for key, value := range a.acc {
		if value.didItWentMoreThan(6) {
			v = append(v, key)
		}
	}
	utils.LogError(a.q.SendMessage(Accumulator{Stations: v, Key: a.id}, ""), "could not send data to accumulator")
}
