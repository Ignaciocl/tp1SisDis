package main

import (
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
)

type cleanable interface {
	Clear()
}

type actionable struct {
	acc map[string]dStation
	q   queue.Sender[Accumulator]
	id  string
	c   []cleanable
}

func (a actionable) DoActionIfEOF() {
	v := make([]string, 0, len(a.acc))
	for key, value := range a.acc {
		if value.didItWentMoreThan(6) {
			v = append(v, key)
		}
	}
	utils.LogError(a.q.SendMessage(Accumulator{Stations: v, ClientID: a.id}, ""), "could not send data to accumulator")
	for _, c := range a.c {
		c.Clear()
	}
	for k := range a.acc {
		delete(a.acc, k)
	}
}
