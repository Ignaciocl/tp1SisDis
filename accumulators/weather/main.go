package main

import (
	"encoding/json"
	"errors"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/fileManager"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/keyChecker"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strings"
)

const (
	serviceName        = "accumulator-weather"
	storageFilename    = "weather_accumulator.csv"
	eofStorageFilename = "eof.csv"
)

type t3 struct{}

func (t t3) ToWritable(data *Receivable) []byte {
	returnable, _ := json.Marshal(data)
	return returnable
}

func (t t3) FromWritable(d []byte) *Receivable {
	data := strings.Split(string(d), Sep)[0]
	var r Receivable
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		utils.LogError(err, "could not unmarshal from db")
	}
	return &r
}
func (r Receivable) GetId() int64 {
	return 0
}

func (r Receivable) SetId(id int64) {

}

func main() {
	id := os.Getenv("id")
	inputQueue, _ := queue.InitializeReceiver[Receivable]("weatherAccumulator", "rabbit", id, "", nil)
	aq, _ := queue.InitializeSender[AccumulatorData]("accumulator", 0, nil, "rabbit")
	sfe, _ := common.CreateConsumerEOF([]common.NextToNotify{{"accumulator", aq}}, "weatherAccumulator", inputQueue, 3) //  three cities
	grace, _ := common.CreateGracefulManager("rabbit")
	counter := 0
	acc := WeatherDuration{
		Total:    0,
		Duration: 0,
		Id:       -1,
	}
	db, _ := fileManager.CreateDB[*WeatherDuration](t{}, storageFilename, 300, Sep)
	other, _ := fileManager.CreateDB[*Receivable](t3{}, "pepe.csv", 500, Sep)
	if accumulated, err := db.ReadLine(); err == nil {
		acc = *accumulated
	}
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer sfe.Close()
	defer inputQueue.Close()
	defer aq.Close()
	ik, err := keyChecker.CreateIdempotencyChecker(5000)
	eofDb, err := fileManager.CreateDB[*eofData](t2{}, eofStorageFilename, 300, Sep)
	utils.FailOnError(err, "could not create idempotency checker")
	act := actionable{
		acc: &acc,
		q:   aq,
		key: id,
		c:   []cleanable{eofDb},
	}
	utils.FailOnError(propagateEof(eofDb, &act, sfe), "could not propagate eof")
	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			utils.LogError(other.Write(&data), "could not write")
			// log.Infof("element received")
			counter += 1
			if err != nil {
				utils.LogError(err, "error while reading message")
				continue
			}
			if data.EOF {
				if !strings.HasSuffix(data.IdempotencyKey, id) {
					log.Infof("eof received from another client: %s, not propagating", data.IdempotencyKey)
					utils.LogError(inputQueue.AckMessage(msgId), "could not acked message")
					continue
				}
				log.Infof("eof is received: %s", data.IdempotencyKey)
				utils.LogError(eofDb.Write(&eofData{
					IdempotencyKey: id,
					Id:             0,
				}), "couldn't write into eof db")
				sfe.AnswerEofOk(id, &act)
				utils.LogError(inputQueue.AckMessage(msgId), "error while acking msg")
				log.Infof("%d is amount used", counter)
				counter = 0
				continue
			}
			if ik.IsKey(data.IdempotencyKey) {
				counter -= 1
				log.Infof("%v already exists on map", data)
				utils.LogError(inputQueue.AckMessage(msgId), "error while acking msg")
				continue
			}
			if data.IdempotencyKey == acc.LastIdempotencyKey {
				counter -= 1
				log.Infof("repeated idempotency key %s", data.IdempotencyKey)
				utils.LogError(ik.AddKey(data.IdempotencyKey), "could not add idempotency key")
				utils.LogError(inputQueue.AckMessage(msgId), "error while acking msg")
				continue
			}
			acc.Total += data.Data.Total
			acc.Duration += data.Data.Duration
			acc.LastIdempotencyKey = data.IdempotencyKey
			utils.LogError(db.Write(&acc), "could not update db on weather")
			utils.LogError(ik.AddKey(data.IdempotencyKey), "could not add idempotency key")
			utils.LogError(inputQueue.AckMessage(msgId), "error while acking msg")
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName + id)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "health check error")
	}()

	grace.WaitForSigterm()
}

func propagateEof(db fileManager.Manager[*eofData], act *actionable, sfe common.WaitForEof) error {
	for {
		line, err := db.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		sfe.AnswerEofOk(line.IdempotencyKey, act)
	}
	return nil
}
