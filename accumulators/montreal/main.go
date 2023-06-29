package main

import (
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/fileManager"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/keyChecker"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
)

const (
	serviceName        = "accumulator-montreal"
	storageFilename    = "montreal_accumulator.csv"
	eofStorageFilename = "eof.csv"
)

type AccumulatorData struct {
	EndingStation string  `json:"ending_station"`
	Distance      float64 `json:"distance"`
}

type AccumulatorInfo struct {
	Data []AccumulatorData `json:"data"`
	common.EofData
}

type Accumulator struct {
	ClientID string   `json:"client_id"`
	Stations []string `json:"stations"`
	common.EofData
}

func processData(data AccumulatorData, m map[string]dStation) dStation {
	station, ok := m[data.EndingStation]
	if !ok {
		station = dStation{
			Counter:         0,
			DistanceCounted: 0,
		}
	}
	station.add(data.Distance)
	m[data.EndingStation] = station
	return station
}

func main() {
	id := os.Getenv("id")
	if id == "" {
		panic("missing montreal accumulator id")
	}
	amountCalc, err := strconv.Atoi(os.Getenv("calculators"))
	utils.FailOnError(err, "missing env value of calculator")
	db, err := fileManager.CreateDB[*dStation](t{}, storageFilename, 300, Sep)
	utils.FailOnError(err, "could not create db")
	acc := make(map[string]dStation)
	eofDb, err := fileManager.CreateDB[*eofData](t2{}, eofStorageFilename, 300, Sep)
	// todo fillData missing
	ik, err := keyChecker.CreateIdempotencyChecker(20)
	utils.FailOnError(err, "could not create db")
	inputQueue, _ := queue.InitializeReceiver[AccumulatorInfo]("preAccumulatorMontreal", "rabbit", id, "", nil)
	outputQueue, _ := queue.InitializeSender[Accumulator]("accumulator", 0, nil, "rabbit")
	me, _ := common.CreateConsumerEOF([]common.NextToNotify{{"accumulator", outputQueue}}, "preAccumulatorSt", inputQueue, amountCalc)
	grace, _ := common.CreateGracefulManager("rabbit")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer me.Close()
	defer inputQueue.Close()
	defer outputQueue.Close()
	go func() {
		for {
			dataInfo, msgId, err := inputQueue.ReceiveMessage()
			if dataInfo.EOF {
				utils.LogError(eofDb.Write(&eofData{
					IdempotencyKey: id,
					Id:             0,
				}), "could not update eof database")
				me.AnswerEofOk(id, actionable{
					q:   outputQueue,
					acc: acc,
					id:  id,
				})
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}

			if ik.IsKey(dataInfo.IdempotencyKey) {
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}
			for i, d := range dataInfo.Data {
				idk := fmt.Sprintf("%s-%d", dataInfo.IdempotencyKey, i)
				if data, ok := acc[d.EndingStation]; ok && !checkIdempotencyKey(idk, data) {
					continue
				}
				station := processData(d, acc)
				station.LastIdempotencyKey = idk
				utils.LogError(db.Write(&station), "could not update db")
				acc[d.EndingStation] = station
			}
			utils.LogError(ik.AddKey(dataInfo.IdempotencyKey), "failed while trying to add to ik store")
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName + id)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "health check error")
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

func checkIdempotencyKey(ik string, d dStation) bool {
	lastIdempotencyDecompress := strings.Split(d.LastIdempotencyKey, "-")
	lastIKDecompress := strings.Split(ik, "-")
	return ik != d.LastIdempotencyKey &&
		((lastIdempotencyDecompress[0] != lastIKDecompress[0]) ||
			(lastIKDecompress[1] > lastIdempotencyDecompress[1]))
}
