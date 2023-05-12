package main

import (
	"encoding/json"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
)

type eofRequest struct {
	File string `json:"file"`
	City string `json:"city"`
}

type sigterm struct {
	Sigterm bool `json:"sigterm"`
}

func main() {
	const distributorAmount = 3
	const workerAmount = 3
	p, _ := common.CreatePublisher("rabbit")
	eofListener, _ := common.InitializeRabbitQueue[any, any]("eofListener", "rabbit")
	eofStarterQ, _ := common.InitializeRabbitQueue[eofRequest, eofRequest]("eofStarter", "rabbit")
	qPanic, _ := common.CreateGracefulManager("rabbit")
	defer p.Close()
	defer eofListener.Close()
	defer eofStarterQ.Close()
	defer qPanic.Close()
	finish := make(chan struct{}, 1)
	eofUsed := make(chan struct{}, 1)
	eofCount := 0
	cancelChan := make(chan os.Signal, 1)
	dataChan := make(chan eofRequest, 10)
	go func() {
		for {
			eofData, _ := eofStarterQ.ReceiveMessage()
			log.Printf("data is: %v\n", eofData)
			dataChan <- eofData
		}
	}()
	go func() {
		var eofData eofRequest
		for {
			eofData = <-dataChan
			log.Println("commencing flow")
			d, _ := json.Marshal(eofData)
			p.Publish("distributorEOF", d)
			for {
				<-eofUsed
				eofCount += 1
				if eofCount > distributorAmount-1 {
					eofCount = 0
					break
				}
			}
			if eofData.File == "stations" {
				log.Printf("sending %v\n", eofData)
				p.Publish("workerStations", d)
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > workerAmount-1 {
						eofCount = 0
						break
					}
				}
				log.Println("continue after stations")
				p.Publish("joinerMontreal", d)
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > 0 {
						eofCount = 0
						break
					}
				}
				log.Println("2")
				p.Publish("joinerStations", d)
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > 0 {
						eofCount = 0
						break
					}
				}
				log.Println("3")
				p.Publish("accumulatorMontreal", d)
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > 0 {
						break
					}
				}
				log.Println("4")
				p.Publish("accumulatorEOF", d)
				log.Println("5")
			}
			if eofData.File == "weather" {
				d, _ := json.Marshal(eofData)
				p.Publish("workerWeather", d)
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > workerAmount-1 {
						eofCount = 0
						break
					}
				}
				p.Publish("joinerWeather", d)
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > 0 {
						eofCount = 0
						break
					}
				}
				p.Publish("accumulatorEOF", d)
			}
			if eofData.File == "trips" {
				d, _ := json.Marshal(eofData)
				p.Publish("workerTrips", d)
				log.Println("1a")
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > workerAmount-1 {
						eofCount = 0
						break
					}
				}
				p.Publish("joinerStations", d)
				log.Println("2a")
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > 0 {
						eofCount = 0
						break
					}
				}
				p.Publish("joinerMontreal", d)
				log.Println("3a")
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > 0 {
						eofCount = 0
						break
					}
				}
				p.Publish("joinerWeather", d)
				log.Println("4a")
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > 0 {
						eofCount = 0
						break
					}
				}
				p.Publish("accumulatorMontreal", d)
				log.Println("5a")
				for {
					<-eofUsed
					eofCount += 1
					if eofCount > 0 {
						break
					}
				}
				p.Publish("accumulatorEOF", d)
			}
		}
	}()
	go func() {
		for {
			_, _ = eofListener.ReceiveMessage()
			eofUsed <- struct{}{}
		}
	}()
	go func() {
		qPanic.WaitForSigterm()
		log.Printf("sigterm was called, finishing manager")
		finish <- struct{}{}
	}()
	go func() {
		<-cancelChan
		s := sigterm{Sigterm: true}
		d, _ := json.Marshal(s)
		p.Publish("sigterm", d)
		finish <- struct{}{}
	}()
	<-finish
}
