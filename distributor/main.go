package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type receivedData struct {
	File string        `json:"file"`
	Data []interface{} `json:"data"`
	City string        `json:"city,omitempty"`
}

type SendableData struct {
	City string      `json:"city"`
	Data interface{} `json:"data,omitempty"` // Why would i decompress here? it is just a distributor
	Key  string      `json:"key"`
	EOF  bool        `json:"EOF"`
}

func SendMessagesToQueue(data []interface{}, queue common.Queue[SendableData, SendableData], city string) {
	for _, v := range data {
		err := queue.SendMessage(SendableData{
			City: city,
			Data: v,
			Key:  "random",
			EOF:  false,
		})
		if err != nil {
			log.Printf("error while sending message: %v with data %v", err, data)
		}
	}
}

type checker struct {
	currentData map[string]string
	blocker     chan struct{}
	q           common.Queue[receivedData, receivedData]
}

func vInMap(s string, m map[string]string) bool {
	_, ok := m[s]
	return ok
}
func (c checker) IsStillUsingNecessaryDataForFile(file string, city string) bool {
	for {
		<-c.blocker
		if file == "trips" && city == "washington" {
			log.Printf("boolean is %v\n", c.q.IsEmpty())
		}
		if (file != c.currentData["file"] && vInMap(file, c.currentData) && vInMap(city, c.currentData)) || (file == "trips" && city == "washington" && c.q.IsEmpty()) {
			break
		}
	}
	return true
}

func main() {

	inputQueue, _ := common.InitializeRabbitQueue[receivedData, receivedData]("distributor", "rabbit")
	wfe, _ := common.CreateConsumerEOF("rabbit", "distributorEOF")

	cancelChan := make(chan os.Signal, 1)
	m := make(map[string]string)
	m["file"] = ""
	fileChange := make(chan struct{}, 1)
	fileChange <- struct{}{}
	c := checker{blocker: fileChange, currentData: m, q: inputQueue}
	go func() {
		wfe.AnswerEofOk(c)
	}()
	defer inputQueue.Close()
	defer wfe.Close()
	// catch SIGETRM or SIGINTERRUPT
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		pFile := ""
		var queue common.Queue[SendableData, SendableData]
		for {
			data, err := inputQueue.ReceiveMessage()
			if err != nil {
				common.FailOnError(err, "Failed while receiving message")
				continue
			}
			m[data.File] = ""
			m[data.City] = ""
			m["file"] = data.File
			if pFile != data.File {

				pFile = data.File
				if pFile == "weather" {
					queue, _ = common.InitializeRabbitQueue[SendableData, SendableData]("weatherWorkers", "rabbit")
				} else if pFile == "trips" {
					queue, _ = common.InitializeRabbitQueue[SendableData, SendableData]("tripWorkers", "rabbit")
				} else if pFile == "stations" {
					queue, _ = common.InitializeRabbitQueue[SendableData, SendableData]("stationWorkers", "rabbit")
				}
			}
			fileChange <- struct{}{}
			SendMessagesToQueue(data.Data, queue, data.City)
		}
	}()
	<-fileChange
	<-cancelChan
	log.Printf("Closing for sigterm received")
}
