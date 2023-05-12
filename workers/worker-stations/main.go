package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Station struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Year      int    `json:"yearid,string"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
}

type WorkerStation struct {
	City string  `json:"city"`
	Data Station `json:"data,omitempty"`
	Key  string  `json:"key"`
	EOF  bool    `json:"EOF"`
}

type SendableDataStation struct {
	Code      string `json:"code"`
	Name      string `json:"name"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
}

type JoinerDataStation struct {
	Data SendableDataStation `json:"stationData"`
	Name string              `json:"name"`
	Key  string              `json:"key"`
	EOF  bool                `json:"EOF"`
}

const MontrealStation = "montreal"

func processData(station WorkerStation, qm, qs common.Queue[JoinerDataStation, JoinerDataStation]) {
	js := JoinerDataStation{
		Data: SendableDataStation{
			Code:      station.Data.Code,
			Name:      station.Data.Name,
			Longitude: station.Data.Longitude,
			Latitude:  station.Data.Latitude,
		},
		Key: station.Key,
		EOF: station.EOF,
	}
	if station.City == MontrealStation {
		err := qm.SendMessage(js)
		if err != nil {
			common.FailOnError(err, "Couldn't send message to joiner montreal, failing horribly")
			// ToDo implement shutDown manager
		}
	}
	if station.Data.Year == 2016 || station.Data.Year == 2017 {
		log.Printf("station to send is: %v\n", station)
		err := qs.SendMessage(js)
		if err != nil {
			common.FailOnError(err, "Couldn't send message to joiner stations, failing horribly")
			// ToDo implement shutDown manager
		}
	}
}

type checker struct {
	data      map[string]string
	blocker   chan struct{}
	q         common.Queue[WorkerStation, WorkerStation]
	filesUsed int
}

func (c checker) IsStillUsingNecessaryDataForFile(file string, city string) bool {
	if city != "washington" {
		<-c.blocker
	} else {
		for {
			<-c.blocker
			if c.q.IsEmpty() {
				break
			}
		}
	}
	return true
}

func main() {

	inputQueue, _ := common.InitializeRabbitQueue[WorkerStation, WorkerStation]("stationWorkers", "rabbit")
	outputQueueMontreal, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("montrealQueue", "rabbit")
	outputQueueStations, _ := common.InitializeRabbitQueue[JoinerDataStation, JoinerDataStation]("stationsQueue", "rabbit")
	wfe, _ := common.CreateConsumerEOF("rabbit", "workerStations")
	defer inputQueue.Close()
	defer outputQueueMontreal.Close()
	defer outputQueueStations.Close()
	defer wfe.Close()

	eofCheck := map[string]string{}
	eofCheck["city"] = ""
	blocker := make(chan struct{}, 1)
	blocker <- struct{}{}
	c := checker{data: eofCheck, blocker: blocker, filesUsed: 0, q: inputQueue}
	go func() {
		wfe.AnswerEofOk(c)
	}()

	oniChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(oniChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		pCity := MontrealStation
		for {
			data, err := inputQueue.ReceiveMessage()
			log.Printf("station to send is: %v\n", data)
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			if data.City != pCity || pCity == "washington" {
				pCity = data.City
				blocker <- struct{}{}
			}
			processData(data, outputQueueMontreal, outputQueueStations)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-oniChan
	log.Printf("Closing for sigterm received")
}
