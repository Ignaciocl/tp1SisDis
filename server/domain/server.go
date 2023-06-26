package domain

import (
	"encoding/json"
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/client"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"server/internal/config"
	"server/internal/dataentities"
)

const (
	serviceName       = "server"
	userID            = "random"
	montrealCity      = "montreal"
	torontoCity       = "toronto"
	washingtonCity    = "washington"
	minimumBodyLength = 3
)

type closer interface {
	Close() error
}

type Server struct {
	config *config.ServerConfig
}

func NewServer(config *config.ServerConfig) *Server {
	return &Server{
		config: config,
	}
}

func (s *Server) Run() error {
	// Initialize clients
	inputDataAddress := fmt.Sprintf("%s:%v", serviceName, s.config.InputPort)
	outputDataAddress := fmt.Sprintf("%s:%v", serviceName, s.config.OutputPort)

	dataReceiverSocket := client.NewSocket(client.NewSocketConfig(
		s.config.Protocol,
		inputDataAddress,
		s.config.PacketLimit,
	))
	defer closeService(dataReceiverSocket)

	queryReplierSocket := client.NewSocket(client.NewSocketConfig(
		s.config.Protocol,
		outputDataAddress,
		s.config.PacketLimit,
	))
	defer closeService(queryReplierSocket)

	// Initialize queues and others
	senderConfig := s.config.Sender
	sender, err := queue.InitializeSender[dataentities.DataToSend](senderConfig.Consumer, senderConfig.AmountOfDistributors, nil, s.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("%w: %v", errInitializingSender, err)
	}
	defer closeService(sender)

	eofStarter, err := common.CreatePublisher(s.config.ConnectionString, sender)
	if err != nil {
		return fmt.Errorf("%w: %v", errCreatingPublisher, err)
	}
	defer eofStarter.Close()

	receiverConfig := s.config.Receiver
	accumulatorInfo, err := queue.InitializeReceiver[dataentities.AccumulatorData](receiverConfig.Queue, s.config.ConnectionString, receiverConfig.RoutingKey, receiverConfig.Topic, sender)
	if err != nil {
		return fmt.Errorf("%w: %v", errInitializingReceiver, err)
	}
	defer closeService(accumulatorInfo)

	gracefulManager, err := common.CreateGracefulManager(s.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("%w: %v", errInitializingGracefulManager, err)
	}
	defer gracefulManager.Close()
	defer common.RecoverFromPanic(gracefulManager, "")

	log.Info("waiting for clients")
	// ToDo: if we have time, we can use a semaphore from a library...
	sem := make(chan struct{}, 1)
	sem <- struct{}{}

	dataQuery := dataentities.DataQuery{
		Sem:  sem,
		Data: make(map[string]map[string]interface{}, 0),
	}

	go func() {
		result, id, err := accumulatorInfo.ReceiveMessage()
		if err != nil {
			log.Errorf("%s: %v", errReceivingData, err)
			return
		}

		log.Infof("data received from accumulator: %v", result)
		dataQuery.WriteQueryValue(result.QueryResult, userID)
		utils.LogError(accumulatorInfo.AckMessage(id), "could not ack message")
	}()

	go s.receivePolling(queryReplierSocket, dataQuery)
	go s.receiveData(dataReceiverSocket, eofStarter, sender)

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName)
	go func() {
		err := healthCheckerReplier.Run()
		log.Errorf("healtchecker error: %v", err)
	}()
	common.WaitForSigterm(gracefulManager)
	return nil
}

// receiveData2 receives data from multiple clients and send it to the distributor
func (s *Server) receiveData2(receiverSocket client.Client, eofStarter common.Publisher, distributorQueue queue.Sender[dataentities.DataToSend]) {
	err := receiverSocket.StartListener()
	if err != nil {
		log.Error(getLogMessage("error starting listener receiveData", err))
		panic(err)
	}

	clientsSemaphore := newSemaphore(s.config.MaxActiveClients)

	for {
		clientsSemaphore.acquire()
		messageHandler, err := receiverSocket.AcceptNewConnections()
		if err != nil {
			log.Error(getLogMessage("error accepting new connections in receiveData", err))
			clientsSemaphore.release()
			continue
		}

		go func(messageHandler client.MessageHandler) {
			defer clientsSemaphore.release()

			err = s.handleInputData(messageHandler, eofStarter, distributorQueue)
			if err != nil {
				log.Error(getLogMessage("error handling data from client", err))
				return
			}

			log.Infof("All data from client was processed correctly!")
		}(messageHandler)

	}

}

// handleInputData handles the data that comes from a client, the main function of this method it's to send the data to the next stage
func (s *Server) handleInputData(messageHandler client.MessageHandler, eofStarter common.Publisher, distributorQueue queue.Sender[dataentities.DataToSend]) error {
	publishingConfig := s.config.Publisher
	for {
		bodyBytes, err := messageHandler.Listen()
		if err != nil {
			log.Error(getLogMessage("error reading from socket in handleInputData", err))
			continue
		}

		message := string(bodyBytes)
		if utils.Contains[string](message, s.config.InputDataFinMessages) {
			finMessage := getFINMessage(message)
			if finMessage == s.config.FinishProcessingMessage {
				return nil
			}

			messageMetadata := getMetadataFromMessage(message)

		}

	}

}

func (s *Server) receiveData(client client.Client, eofStarter common.Publisher, queue queue.Sender[dataentities.DataToSend]) {
	err := client.StartListener()
	if err != nil {
		log.Error(getLogMessage("error starting listener receiveData", err))
		panic(err)
	}

	messageHandler, err := client.AcceptNewConnections()
	if err != nil {
		log.Error(getLogMessage("error accepting new connections in receiveData", err))
		panic(err)
	}

	eofAmount := 0       // tampoco
	city := montrealCity // Esto ya no necesitamos, sale de la data
	publishingConfig := s.config.Publisher
	for {
		bodyBytes, err := messageHandler.Listen()
		if err != nil {
			log.Error(getLogMessage("error reading from socket in receiveData", err))
			continue
		}
		var data dataentities.FileData

		// Sanity-check
		if len(bodyBytes) < minimumBodyLength {
			continue
		}

		if err := json.Unmarshal(bodyBytes, &data); err != nil {
			utils.FailOnError(err, fmt.Sprintf("error while receiving data for file: %v", string(bodyBytes)))
			continue
		} // esto no hay que hacer, hay que sacar los campos del string para saber que es

		if data.EOF != nil && *data.EOF { // preguntamos si tiene la data de eof
			d, err := json.Marshal(common.EofData{ // Llamamos al constructor de EOF que tiene metadata y esas cosas
				EOF:            true,
				IdempotencyKey: fmt.Sprintf("%s-%s", city, data.File),
			})

			if err != nil {
				log.Error(getLogMessage("error marshalling data in receiveData", err))
				panic(err)
			}

			log.Infof("EOF received from client, to propagate: %v", string(d))
			err = eofStarter.Publish(publishingConfig.Exchange, d, publishingConfig.RoutingKey, publishingConfig.Topic)
			if err != nil {
				log.Error(getLogMessage("error publishing in distributor", err))
				panic(err)
			}

			err = messageHandler.Send([]byte(s.config.FinishProcessingMessage)) // Creo que es el ACK de haber leido x file
			if err != nil {
				log.Error(getLogMessage("error sending finish processing message", err))
				panic(err)
			}

			// Todo esto puede volar, quizas el contador no
			eofAmount += 1
			if eofAmount == 3 {
				city = torontoCity
			} else if eofAmount == 6 {
				city = washingtonCity
			}
			if eofAmount == 9 {
				break
			}
			continue
		}

		err = queue.SendMessage(dataentities.DataToSend{
			File:           data.File,
			Data:           data.Data,
			City:           city,
			IdempotencyKey: "fakeIdempotencyKey", // ToDo: replace with the correct idempotency key. Licha
		}, "")

		if err != nil {
			log.Errorf("error happened: %v", err)
			return
		}

		err = messageHandler.Send([]byte(s.config.KeepTryingMessage))
		if err != nil {
			log.Error(getLogMessage("error sending keep trying message", err))
			panic(err)
		}
	}

	err = client.Close()
	if err != nil {
		log.Error(getLogMessage("error closing connection", err))
		return
	}
	log.Info("connection closed")
}

func (s *Server) receivePolling(pollingSocket client.Client, dataQuery dataentities.DataQuery) {
	log.Info("waiting for polling")
	err := pollingSocket.StartListener()
	if err != nil {
		log.Error(getLogMessage("error starting listener", err))
	}

	messageHandler, err := pollingSocket.AcceptNewConnections()
	if err != nil {
		log.Error(getLogMessage("error accepting new connections", err))
		return
	}
	log.Info("received connection")

	for {
		log.Debug("waiting for polling of client")
		dataBytes, err := messageHandler.Listen()
		if err != nil {
			log.Error(getLogMessage("error receiving data", err))
			return
		}

		log.Debugf("Data: %s", string(dataBytes)) // ToDo: do something with data
		log.Infof("polling of client receive correctly")
		if data, ok := dataQuery.GetQueryValue(userID); !ok {
			err = messageHandler.Send([]byte("{}"))
			if err != nil {
				log.Error(getLogMessage("error sending response from polling", err))
				return
			}

		} else {
			dataMap, err := json.Marshal(data)
			if err != nil {
				log.Error(getLogMessage("error marshalling data", err))
				return
			}
			err = messageHandler.Send(dataMap)
			if err != nil {
				log.Error(getLogMessage("error sending data map from polling", err))
				return
			}
			break
		}
	}
}

// closeService close the given service. If an error occurs it will  be logged
func closeService(service closer) {
	err := service.Close()
	if err != nil {
		log.Errorf("")
		return
	}
}

func getLogMessage(message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[service: %s][status: ERROR] %s: %v", serviceName, message, err)
	}

	return fmt.Sprintf("[service: %s][status: OK] %s", serviceName, message)
}
