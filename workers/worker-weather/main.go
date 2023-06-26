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
	"worker-weather/internal/dtos"
	weatherErrors "worker-weather/internal/errors"
	"worker-weather/internal/filter"
	"worker-weather/internal/transformer"
)

const (
	idEnvVar         = "id"
	logLevelEnvVar   = "LOG_LEVEL"
	defaultLogLevel  = "INFO"
	serviceName      = "worker-weather"
	connectionString = "rabbit"
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
	logLevel := os.Getenv(logLevelEnvVar)
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	if err := InitLogger(logLevel); err != nil {
		panic(fmt.Sprintf("error initializing logger: %v", err))
	}

	id := os.Getenv(idEnvVar)
	if id == "" {
		panic("missing weather worker id")
	}

	distributors, err := strconv.Atoi(os.Getenv("distributors"))
	utils.FailOnError(err, "missing env value of distributors")
	inputQueue, _ := queue.InitializeReceiver[dtos.InputData]("weatherWorkers", connectionString, id, "", nil)
	outputQueueWeather, _ := queue.InitializeSender[dtos.JoinerData]("weatherQueue", 0, inputQueue, "")
	v := make([]common.NextToNotify, 1)
	v = append(v, common.NextToNotify{
		Name:       "weatherQueue",
		Connection: outputQueueWeather,
	})
	iqEOF, _ := common.CreateConsumerEOF(v, "weatherWorkers", inputQueue, distributors)
	grace, _ := common.CreateGracefulManager(connectionString)
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer iqEOF.Close()
	defer inputQueue.Close()
	defer outputQueueWeather.Close()

	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
			}

			metadata := data.Metadata

			if metadata.IsEOF() {
				// ToDo: nacho que va aca?
				iqEOF.AnswerEofOk(metadata.GetIdempotencyKey(), nil)
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}

			// Now we have a batch with weather data
			for _, rawWeatherData := range data.Data {
				// 1. Transform the raw data into WeatherData
				weatherData, err := transformer.Transform(rawWeatherData)
				if err != nil {
					if errors.Is(err, weatherErrors.ErrInvalidWeatherData) {
						log.Debug("Invalid weather data")
						continue
					}
					log.Errorf("Error transforming raw weather data: %v", err)
					panic(err)
				}

				// 2. Get extra data contain in the message
				rawMessageExtraData := utils.GetMetadataFromMessage(rawWeatherData)
				metadata.IdempotencyKey = fmt.Sprintf( //clientID-batchNum-messageNum-city
					"%s-%s-%s-%s",
					rawMessageExtraData.ClientID,
					rawMessageExtraData.BatchNumber,
					rawMessageExtraData.MessageNumber,
					rawMessageExtraData.City,
				)

				// 3. Check if the Weather Data is valid to go to the next stage
				if filter.IsValid(*weatherData) {
					sendData(rawMessageExtraData.ClientID, *weatherData, metadata, outputQueueWeather)
					continue
				}
				log.Debugf("Filtering weather data: %+v", *weatherData)
			}

			err = inputQueue.AckMessage(msgId)
			if err != nil {
				utils.FailOnError(err, "error ACK weather data")
			}
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName + id)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "healtchecker error")
	}()

	log.Info(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

// sendData sends weather data to the next stage through the given sender
func sendData(clientID string, weatherData dtos.WeatherData, metadata commonDtos.Metadata, sender queue.Sender[dtos.JoinerData]) {
	dataToSend := dtos.JoinerData{
		City:           metadata.GetCity(),
		IdempotencyKey: metadata.GetIdempotencyKey(),
		EOF:            metadata.IsEOF(),
		Data:           weatherData,
	}

	err := sender.SendMessage(dataToSend, clientID)
	if err != nil {
		utils.FailOnError(err, "Couldn't send message to weather joiner, failing horribly")
	}
}
