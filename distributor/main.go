package main

import (
	"distributor/internal/config"
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/dtos"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"os"
)

const (
	logLevelEnvVar  = "LOG_LEVEL"
	defaultLogLevel = "DEBUG"
	idEnvVar        = "id"
	serviceName     = "distributor"
	weatherData     = "weather"
	stationsData    = "stations"
	tripsData       = "trips"
)

// inputOutputData data that comes from the server and also the data that is sent to the next stage
type inputOutputData struct {
	Metadata dtos.Metadata `json:"metadata"`
	Data     []string      `json:"data"`
}

/*
DELETE THIS WHEN IS SAFE
func SendMessagesToQueue(data []interface{}, queue queue.Sender[SendableData], city string) {
	for _, v := range data {
		err := queue.SendMessage(SendableData{
			City: city,
			Data: v,
			ClientID:  "1",
		}
		d.EOF = false
		d.IdempotencyKey = fmt.Sprintf("%s-%d", ik, i)
		err := queue.SendMessage(d, "")
		if err != nil {
			log.Errorf("error while sending a message to next step")
		}
	}
}*/

// InitLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	customFormatter := &log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   false,
	}
	log.SetFormatter(customFormatter)
	log.SetLevel(level)
	return nil
}

func main() {
	logLevel := os.Getenv(logLevelEnvVar)
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	if err := InitLogger(logLevel); err != nil {
		panic(fmt.Sprintf("error initializing logger: %v", err))
	}

	id := os.Getenv(idEnvVar)
	if id == "" {
		panic("missing distributor ID")
	}

	distributorConfig, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}
	workerQueues := distributorConfig.WorkerQueues
	maxAmountReceivers := distributorConfig.MaxAmountReceivers
	necessaryAmount := distributorConfig.NecessaryAmount

	inputQueue, err := queue.InitializeReceiver[inputOutputData](serviceName, distributorConfig.ConnectionString, id, "", nil)
	if err != nil {
		panic(err)
	}
	defer closeService(inputQueue)

	weatherWorkerQueue, err := queue.InitializeSender[inputOutputData](workerQueues[weatherData], maxAmountReceivers, nil, distributorConfig.ConnectionString)
	if err != nil {
		panic(err)
	}
	defer closeService(weatherWorkerQueue)

	tripsWorkerQueue, err := queue.InitializeSender[inputOutputData](workerQueues[tripsData], maxAmountReceivers, nil, distributorConfig.ConnectionString)
	if err != nil {
		panic(err)
	}
	defer closeService(tripsWorkerQueue)

	stationsWorkerQueue, err := queue.InitializeSender[inputOutputData](workerQueues[stationsData], maxAmountReceivers, nil, distributorConfig.ConnectionString)
	if err != nil {
		panic(err)
	}
	defer closeService(stationsWorkerQueue)

	weatherWorkerEOFQueue, err := common.CreateConsumerEOF([]common.NextToNotify{{Name: workerQueues[weatherData], Connection: weatherWorkerQueue}}, serviceName, inputQueue, necessaryAmount)
	if err != nil {
		panic(err)
	}
	defer weatherWorkerEOFQueue.Close()

	tripsWorkerEOFQueue, err := common.CreateConsumerEOF([]common.NextToNotify{{Name: workerQueues[tripsData], Connection: tripsWorkerQueue}}, serviceName, inputQueue, necessaryAmount)
	if err != nil {
		panic(err)
	}
	defer tripsWorkerEOFQueue.Close()

	stationsWorkerEOFQueue, err := common.CreateConsumerEOF([]common.NextToNotify{{Name: workerQueues[stationsData], Connection: stationsWorkerQueue}}, serviceName, inputQueue, necessaryAmount)
	if err != nil {
		panic(err)
	}
	defer stationsWorkerEOFQueue.Close()

	gracefulManager, err := common.CreateGracefulManager(distributorConfig.ConnectionString)
	if err != nil {
		panic(err)
	}
	defer gracefulManager.Close()
	defer common.RecoverFromPanic(gracefulManager, "")
	// catch SIGETRM or SIGINTERRUPT

	go func() {
		var sender queue.Sender[inputOutputData]
		var eofManager common.WaitForEof

		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}

			metadata := data.Metadata

			if metadata.IsEOF() {
				dataType := metadata.GetDataType()
				if dataType == tripsData {
					eofManager = tripsWorkerEOFQueue
				} else if dataType == stationsData {
					eofManager = stationsWorkerEOFQueue
				} else if dataType == weatherData {
					eofManager = weatherWorkerEOFQueue
				} else {
					utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
					continue
				}

				log.Infof("eof received and distributed for %v", data)

				eofManager.AnswerEofOk(metadata.IdempotencyKey, nil)
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}

			// Get sender based on dataType
			switch metadata.GetDataType() {
			case weatherData:
				sender = weatherWorkerQueue
			case stationsData:
				sender = stationsWorkerQueue
			case tripsData:
				sender = tripsWorkerQueue
			default:
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}

			err = sender.SendMessage(data, "")
			if err != nil {
				utils.LogError(err, "error sending message to worker")
			}

			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName + id)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "health check error")
	}()

	common.WaitForSigterm(gracefulManager)
}

type closer interface {
	Close() error
}

func closeService(service closer) {
	err := service.Close()
	if err != nil {
		log.Errorf("error closing service: %v", err)
	}
}
