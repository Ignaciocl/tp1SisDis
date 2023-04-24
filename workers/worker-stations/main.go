package main

import (
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"log"
)

type msg struct {
	Intent string `json:"intent"`
	Data   any    `json:"data"`
}

func main() {

	queue, _ := common.InitializeRabbitQueue[msg, msg]("stationWorkers", "rabbit")

	var forever chan struct{}
	go func() {
		log.Printf("smt")
		for {
			log.Printf("loop")
			if mr, err := queue.ReceiveMessage(); err != nil {
				common.FailOnError(err, "i failed")
			} else {
				log.Printf("Received a message with intent: %s and body: %v", mr.Intent, mr.Data)
			}
		}
		log.Printf("else")
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
}
