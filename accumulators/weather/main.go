package main

import (
	"encoding/json"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/fileManager"
	"github.com/Ignaciocl/tp1SisdisCommons/keyChecker"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

type WeatherDuration struct {
	Total              int    `json:"total"`
	Duration           int    `json:"duration"`
	Id                 int64  `json:"id"`
	LastIdempotencyKey string `json:"last_idempotency_key"`
}
type Receivable struct {
	Data WeatherDuration `json:"data"`
	common.EofData
	Key string `json:"key"`
}

func (r *WeatherDuration) GetId() int64 {
	return r.Id
}

func (r *WeatherDuration) SetId(id int64) {
	r.Id = id
}

type AccumulatorData struct {
	Dur float64 `json:"duration"`
	Key string  `json:"key"`
	common.EofData
}

type t struct {
}

func (t t) ToWritable(data *WeatherDuration) []byte {
	d, _ := json.Marshal(data)
	return d
}

func (t t) FromWritable(d []byte) *WeatherDuration {
	data := strings.Split(string(d), "-impos-")[0]
	var r WeatherDuration
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		utils.LogError(err, "could not unmarshal from db")
	}
	return &r
}

func main() {
	id := os.Getenv("id")
	inputQueue, _ := queue.InitializeReceiver[Receivable]("weatherAccumulator", "rabbit", id, "", nil)
	aq, _ := queue.InitializeSender[AccumulatorData]("accumulator", 0, nil, "rabbit")
	sfe, _ := common.CreateConsumerEOF([]common.NextToNotify{{"accumulator", aq}}, "weatherAccumulator", inputQueue, 3) //  three cities
	grace, _ := common.CreateGracefulManager("rabbit")
	db, _ := fileManager.CreateDB[*WeatherDuration](t{}, "cambiameAcaLicha", 300, "-impos-")
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer sfe.Close()
	defer inputQueue.Close()
	defer aq.Close()
	ik, err := keyChecker.CreateIdempotencyChecker(10)
	utils.FailOnError(err, "could not create idempotency checker")
	acc := WeatherDuration{
		Total:    0,
		Duration: 0,
		Id:       -1,
	}
	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if err != nil {
				utils.LogError(err, "error while reading message")
				continue
			}
			if data.EOF {
				log.Infof("eof is received: %s", data.IdempotencyKey)
				sfe.AnswerEofOk(id, &actionable{
					acc: &acc,
					q:   aq,
					key: id,
				})
				utils.LogError(inputQueue.AckMessage(msgId), "error while acking msg")
				continue
			}
			if ik.IsKey(data.IdempotencyKey) {
				// log.Infof("%v already exists on map", data)
			}
			//if data.IdempotencyKey == acc.LastIdempotencyKey {
			//	utils.LogError(ik.AddKey(data.IdempotencyKey), "could not add idempotency key")
			//	continue
			//}
			acc.Total += data.Data.Total
			acc.Duration += data.Data.Duration
			acc.LastIdempotencyKey = data.IdempotencyKey
			db.Write(&acc)
			utils.LogError(ik.AddKey(data.IdempotencyKey), "could not add idempotency key")
			utils.LogError(inputQueue.AckMessage(msgId), "error while acking msg")
		}
	}()
	grace.WaitForSigterm()
}
