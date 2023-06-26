package main

import (
	"errors"
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/fileManager"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/keyChecker"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
)

const (
	serviceName        = "accumulator-stations"
	storageFilename    = "stations_accumulator.csv"
	eofStorageFilename = "eof.csv"
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
	ClientID    string   `json:"client_id"`
	common.EofData
}

func processData(data JoinerDataStation, acc map[string]stationData, db fileManager.Manager[*stationData]) {
	if data.DataStation == nil {
		return
	}
	currentRunValues := map[string]bool{}
	for _, ds := range data.DataStation {
		_, alreadyUsedThisRun := currentRunValues[ds.Name]
		if d, ok := acc[ds.Name]; ok && (data.IdempotencyKey != d.LastSetIdempotencyKey || alreadyUsedThisRun) {
			currentRunValues[ds.Name] = true
			d.LastSetIdempotencyKey = data.IdempotencyKey
			d.addYear(ds.Year)
			utils.LogError(db.Write(&d), "could not write into db")
			acc[ds.Name] = d
		} else if !ok && ds.Name != "" {
			nd := stationData{
				SweetSixteen:          0,
				SadSeventeen:          0,
				Name:                  ds.Name,
				LastSetIdempotencyKey: data.IdempotencyKey,
			}
			nd.addYear(ds.Year)
			utils.LogError(db.Write(&nd), "could not write into db")
			acc[ds.Name] = nd
		}
	}
}

type actionable struct {
	acc map[string]stationData
	id  string
	aq  queue.Sender[AccumulatorData]
}

func (a actionable) DoActionIfEOF() {
	savedData := make(map[string][]int, 0)
	for _, value := range a.acc {
		if value.wasDouble() && value.Name != "" {
			savedData[value.Name] = []int{value.SweetSixteen, value.SadSeventeen}
		}
	}

	v := make([]string, 0, len(savedData))
	for key, _ := range savedData {
		v = append(v, key)
	}
	l := AccumulatorData{
		AvgStations: v,
		ClientID:    a.id,
		EofData: common.EofData{
			IdempotencyKey: fmt.Sprintf("key:%s-amount:%d", a.id, len(savedData)),
		},
	}

	log.Infof("sending message to accumulator")
	utils.LogError(a.aq.SendMessage(l, ""), "could not send message to accumulator")
}

func main() {
	id := os.Getenv("id")
	db, err := fileManager.CreateDB[*stationData](t{}, storageFilename, 300, Sep)
	utils.FailOnError(err, "could not create db")
	acc := map[string]stationData{}
	eofDb, err := fileManager.CreateDB[*eofData](t2{}, eofStorageFilename, 300, Sep)
	ik, err := keyChecker.CreateIdempotencyChecker(20)
	utils.FailOnError(err, "could not create db")
	inputQueue, _ := queue.InitializeReceiver[JoinerDataStation]("preAccumulatorSt", "rabbit", id, "", nil)
	aq, _ := queue.InitializeSender[AccumulatorData]("accumulator", 0, nil, "rabbit")
	sfe, _ := common.CreateConsumerEOF([]common.NextToNotify{{"accumulator", aq}}, "preAccumulatorSt", inputQueue, 3)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer sfe.Close()
	defer inputQueue.Close()
	defer aq.Close()
	actionableEOF := actionable{
		acc: acc,
		aq:  aq,
		id:  id,
	}
	utils.FailOnError(fillData(acc, db, eofDb, sfe, actionableEOF), "could not fill with data from the db")
	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if data.EOF {
				idempotencyKey := id
				utils.LogError(eofDb.Write(&eofData{
					IdempotencyKey: idempotencyKey,
					Id:             0,
				}), "could not write in eof db")
				sfe.AnswerEofOk(idempotencyKey, actionableEOF)
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			if ik.IsKey(data.IdempotencyKey) {
				// utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				// continue
			}
			processData(data, acc, db)
			utils.LogError(ik.AddKey(data.IdempotencyKey), "could not store idempotency key")
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "health check error")
	}()

	log.Infof("waiting for messages")
	common.WaitForSigterm(grace)
}

func fillData(acc map[string]stationData, db fileManager.Manager[*stationData], eofDb fileManager.Manager[*eofData], sfe common.WaitForEof, eof actionable) error {
	for {
		line, err := db.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		acc[line.Name] = *line
	}
	for {
		line, err := eofDb.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		sfe.AnswerEofOk(line.IdempotencyKey, eof)
	}
}
