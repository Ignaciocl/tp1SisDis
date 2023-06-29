package main

import (
	"errors"
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	commonDtos "github.com/Ignaciocl/tp1SisdisCommons/dtos"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"worker-station/internal/dtos"
	stationErrors "worker-station/internal/errors"
	"worker-station/internal/filter"
	"worker-station/internal/transformer"
)

const (
	idEnvVar         = "id"
	logLevelEnvVar   = "LOG_LEVEL"
	defaultLogLevel  = "INFO"
	serviceName      = "worker-station"
	montrealStation  = "montreal"
	connectionString = "rabbit"
	lowerBoundYear   = 2016
	upperBoundYear   = 2017
)

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
	/*logLevel := os.Getenv(logLevelEnvVar)
	if logLevel == "" {
		logLevel = defaultLogLevel
	}*/
	logLevel := defaultLogLevel

	if err := InitLogger(logLevel); err != nil {
		panic(fmt.Sprintf("error initializing logger: %v", err))
	}

	id := os.Getenv(idEnvVar)
	if id == "" {
		panic("missing station worker id")
	}

	distributors, err := strconv.Atoi(os.Getenv("distributors"))
	utils.FailOnError(err, "missing env value of distributors")
	inputQueue, _ := queue.InitializeReceiver[dtos.InputData]("stationWorkers", connectionString, id, "", nil)
	outputQueueMontreal, _ := queue.InitializeSender[dtos.JoinerDataStation]("montrealQueue", 0, nil, connectionString)
	outputQueueStations, _ := queue.InitializeSender[dtos.JoinerDataStation]("stationsQueue", 0, nil, connectionString)
	v := make([]common.NextToNotify, 2)
	v = append(v, common.NextToNotify{
		Name:       "montrealQueue",
		Connection: outputQueueMontreal,
	}, common.NextToNotify{
		Name:       "stationsQueue",
		Connection: outputQueueStations,
	})

	iqEOF, err := common.CreateConsumerEOF(v, "stationWorkers", inputQueue, distributors)
	utils.FailOnError(err, "could not use consumer")
	grace, _ := common.CreateGracefulManager(connectionString)
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer iqEOF.Close()
	defer inputQueue.Close()
	defer outputQueueMontreal.Close()
	defer outputQueueStations.Close()

	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}

			metadata := data.Metadata

			if data.EOF {
				log.Infof("eof received to be triggered: %v", data)
				iqEOF.AnswerEofOk(data.IdempotencyKey, nil)
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}

			// Now we have a batch with station data
			for _, rawStationData := range data.Data {

				// 1. Transform the raw data into StationData
				stationData, err := transformer.Transform(rawStationData)
				if err != nil {
					if errors.Is(err, stationErrors.ErrInvalidStationData) {
						log.Debug("Invalid station data")
						continue
					}
					log.Errorf("Error transforming raw station data: %v", err)
					panic(err)
				}

				// 2. Get extra data contain in the message
				rawMessageExtraData := utils.GetMetadataFromMessage(rawStationData)
				metadata.IdempotencyKey = fmt.Sprintf( //clientID-batchNum-messageNum-city
					"%s-%s-%s-%s",
					rawMessageExtraData.ClientID,
					rawMessageExtraData.BatchNumber,
					rawMessageExtraData.MessageNumber,
					rawMessageExtraData.City,
				)

				// 3. Check if the Station Data is valid to go to the next stage
				if filter.IsValid(*stationData) {
					sendData(rawMessageExtraData.ClientID, *stationData, metadata, outputQueueMontreal, outputQueueStations)
					continue
				}
				log.Debugf("Filtering weather data: %+v", *stationData)
			}

			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName + id)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "healthchecker error")
	}()

	log.Info(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

// sendData sends weather data to the next stage through the given sender
func sendData(clientID string, stationData dtos.StationData, metadata commonDtos.Metadata, montrealSender, yearSender queue.Sender[dtos.JoinerDataStation]) {
	dataToSend := dtos.JoinerDataStation{
		Data:     stationData,
		City:     metadata.GetCity(),
		ClientID: clientID,
		EofData: common.EofData{
			IdempotencyKey: metadata.GetIdempotencyKey(),
			EOF:            metadata.IsEOF(),
		},
	}
	if dataToSend.City == montrealStation {
		err := montrealSender.SendMessage(dataToSend, clientID)
		if err != nil {
			utils.FailOnError(err, "Couldn't send message to joiner montreal, failing horribly")
		}
	}
	if yearInRange(stationData.Year, lowerBoundYear, upperBoundYear) {
		err := yearSender.SendMessage(dataToSend, clientID)
		if err != nil {
			utils.FailOnError(err, "Couldn't send message to Year Joiner, failing horribly")
		}
	}
}

func yearInRange(year int, lowerBoundYear int, upperBoundYear int) bool {
	return lowerBoundYear <= year && year <= upperBoundYear
}
