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
	"worker-trips/internal/dtos"
	tripErrors "worker-trips/internal/errors"
	"worker-trips/internal/filter"
	"worker-trips/internal/transformer"
)

const (
	idEnvVar         = "id"
	logLevelEnvVar   = "LOG_LEVEL"
	defaultLogLevel  = "INFO"
	serviceName      = "worker-trip"
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
	logLevel := os.Getenv(logLevelEnvVar)
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	if err := InitLogger(logLevel); err != nil {
		panic(fmt.Sprintf("error initializing logger: %v", err))
	}

	id := os.Getenv(idEnvVar)
	if id == "" {
		panic("missing Trip Worker ID")
	}

	distributors, err := strconv.Atoi(os.Getenv("distributors"))
	utils.FailOnError(err, "missing env value of distributors")
	inputQueue, _ := queue.InitializeReceiver[dtos.InputData]("tripWorkers", connectionString, id, "", nil)
	outputQueueMontreal, _ := queue.InitializeSender[dtos.JoinerData[dtos.SendableDataMontreal]]("montrealQueueTrip", 0, nil, connectionString)   // Change this, should be empty, also for other workers
	outputQueueStations, _ := queue.InitializeSender[dtos.JoinerData[dtos.SendableDataDuplicates]]("stationsQueueTrip", 0, nil, connectionString) // Change this, should be empty, also for other workers
	outputQueueWeather, _ := queue.InitializeSender[dtos.JoinerData[dtos.SendableDataRainfall]]("weatherQueueTrip", 0, nil, connectionString)     // Change this, should be empty, also for other workers
	v := make([]common.NextToNotify, 0, 3)
	v = append(v, common.NextToNotify{
		Name:       "montrealQueueTrip",
		Connection: outputQueueMontreal,
	}, common.NextToNotify{
		Name:       "stationsQueueTrip",
		Connection: outputQueueStations,
	}, common.NextToNotify{
		Name:       "weatherQueueTrip",
		Connection: outputQueueWeather,
	})

	iqEOF, _ := common.CreateConsumerEOF(v, "tripWorkers", inputQueue, distributors)
	grace, _ := common.CreateGracefulManager(connectionString)
	defer grace.Close()
	defer common.RecoverFromPanic(grace, "")
	defer iqEOF.Close()
	defer inputQueue.Close()
	defer outputQueueMontreal.Close()
	defer outputQueueStations.Close()
	defer outputQueueWeather.Close()

	go func() {
		for {
			data, msgId, err := inputQueue.ReceiveMessage()
			if err != nil {
				utils.FailOnError(err, "Failed while receiving message")
				continue
			}

			metadata := data.Metadata

			if data.EOF {
				iqEOF.AnswerEofOk(data.IdempotencyKey, nil)
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}

			// Transform raw data into TripData
			var tripsData []dtos.TripData
			for _, rawTripData := range data.Data {

				tripData, err := transformer.Transform(rawTripData)
				if err != nil {
					if errors.Is(err, tripErrors.ErrInvalidTripData) {
						log.Debug("Invalid trip data")
						continue
					}
					log.Errorf("Error transforming raw trip data: %v", err)
					panic(err)
				}

				tripsData = append(tripsData, *tripData)
			}

			if len(tripsData) == 0 {
				// Nothing to do
				utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
				continue
			}

			extraData := utils.GetMetadataFromMessage(data.Data[0]) // all have the same client ID

			sendData(
				extraData.ClientID,
				metadata,
				tripsData,
				outputQueueMontreal, outputQueueStations, outputQueueWeather)
			utils.LogError(inputQueue.AckMessage(msgId), "failed while trying ack")
		}
	}()

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName + id)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "healthchecker error")
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	common.WaitForSigterm(grace)
}

// sendData sends data to next stages , only if it's valid, through different senders
func sendData(
	clientID string,
	metadata commonDtos.Metadata,
	tripsData []dtos.TripData,
	montrealSender queue.Sender[dtos.JoinerData[dtos.SendableDataMontreal]],
	duplicatesSender queue.Sender[dtos.JoinerData[dtos.SendableDataDuplicates]],
	rainfallSender queue.Sender[dtos.JoinerData[dtos.SendableDataRainfall]]) {

	buildMontreal := metadata.GetCity() == montrealStation
	batchSize := len(tripsData)

	montrealData := make([]dtos.SendableDataMontreal, 0, batchSize)
	duplicatesData := make([]dtos.SendableDataDuplicates, 0, batchSize)
	rainfallData := make([]dtos.SendableDataRainfall, 0, batchSize)

	for _, trip := range tripsData {
		if buildMontreal {
			montrealData = append(montrealData, dtos.NewSendableDataMontrealFromTrip(trip))
		}

		if yearInRange(trip.Year, lowerBoundYear, upperBoundYear) {
			duplicatesData = append(duplicatesData, dtos.NewSendableDataDuplicatesFromTrip(trip))
		}

		if filter.ValidDuration(trip) {
			rainfallData = append(rainfallData, dtos.NewSendableDataRainfallFromTrip(trip))
		}
	}

	if buildMontreal && len(montrealData) > 0 {
		err := montrealSender.SendMessage(dtos.JoinerData[dtos.SendableDataMontreal]{
			ClientID: clientID,
			Data:     montrealData,
			City:     metadata.GetCity(),
			EofData: common.EofData{
				IdempotencyKey: metadata.GetIdempotencyKey(),
				EOF:            metadata.IsEOF(),
			},
		}, clientID)

		if err != nil {
			utils.FailOnError(err, "Couldn't send message from Trip Worker to Montreal Joiner, failing horribly")
		}
	}

	if len(duplicatesData) > 0 {
		err := duplicatesSender.SendMessage(dtos.JoinerData[dtos.SendableDataDuplicates]{
			ClientID: clientID,
			Data:     duplicatesData,
			City:     metadata.GetCity(),
			EofData: common.EofData{
				IdempotencyKey: metadata.GetIdempotencyKey(),
				EOF:            metadata.IsEOF(),
			},
		}, clientID)

		if err != nil {
			utils.FailOnError(err, "Couldn't send message from Trip Worker to Stations Joiner, failing horribly")
		}
	}

	if len(rainfallData) > 0 {
		err := rainfallSender.SendMessage(dtos.JoinerData[dtos.SendableDataRainfall]{
			ClientID: clientID,
			Data:     rainfallData,
			City:     metadata.GetCity(),
			EofData: common.EofData{
				IdempotencyKey: metadata.GetIdempotencyKey(),
				EOF:            metadata.IsEOF(),
			},
		}, clientID)

		if err != nil {
			utils.FailOnError(err, "Couldn't send message from Trip Worker to Rainfall Joiner, failing horribly")
		}
	}
}

func yearInRange(year int, lowerBoundYear int, upperBoundYear int) bool {
	return lowerBoundYear <= year && year <= upperBoundYear
}
