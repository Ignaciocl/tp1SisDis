package domain

import (
	"encoding/json"
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"github.com/Ignaciocl/tp1SisdisCommons/client"
	"github.com/Ignaciocl/tp1SisdisCommons/concurrency"
	"github.com/Ignaciocl/tp1SisdisCommons/dtos"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/queue"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	log "github.com/sirupsen/logrus"
	"server/internal/config"
	"server/internal/dataentities"
	"strings"
)

const (
	serviceName = "server"
	userID      = "3" // FIXME: delete this variable later
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

		d, _ := json.Marshal(result)
		log.Infof("data received from accumulator: %+v\n%s", result, string(d))
		dataQuery.WriteQueryValue(result.QueryResult, result.ClientId) // FIXME: we need the clientID. Nacho
		utils.LogError(accumulatorInfo.AckMessage(id), "could not ack message")
	}()

	go s.receiveData(dataReceiverSocket, eofStarter, sender)
	go s.sendResponses(queryReplierSocket, dataQuery)

	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(serviceName)
	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "health check error")
	}()
	common.WaitForSigterm(gracefulManager)
	return nil
}

// receiveData receives data from multiple clients and send it to the distributor
func (s *Server) receiveData(receiverSocket client.Client, eofStarter common.Publisher, distributorQueue queue.Sender[dataentities.DataToSend]) {
	err := receiverSocket.StartListener()
	if err != nil {
		log.Error(getLogMessage("error starting listener receiveData", err))
		panic(err)
	}

	clientsSemaphore := concurrency.NewSemaphore(s.config.MaxActiveClients)

	log.Info("Listener started correctly. Waiting for clients!")
	for {
		clientsSemaphore.Acquire()
		messageHandler, err := receiverSocket.AcceptNewConnections()
		if err != nil {
			log.Error(getLogMessage("error accepting new connections in receiveData", err))
			clientsSemaphore.Release()
			continue
		}
		log.Info("Connection with client established!")

		go func(messageHandler client.MessageHandler) {
			defer clientsSemaphore.Release()
			defer closeService(messageHandler)

			err = s.handleInputData(messageHandler, eofStarter, distributorQueue)
			if err != nil {
				log.Error(getLogMessage("error handling data from client", err))
				panic(err)
			}

			log.Infof("All data from client was processed correctly!")
		}(messageHandler)

	}
}

// sendResponses method that sends the responses of the queries to the corresponding client
func (s *Server) sendResponses(senderSocket client.Client, dataQuery dataentities.DataQuery) {
	err := senderSocket.StartListener()
	if err != nil {
		log.Error(getLogMessage("error starting listener in sendResponses", err))
		panic(err)
	}

	clientsSemaphore := concurrency.NewSemaphore(s.config.MaxActiveClients)

	for {
		clientsSemaphore.Acquire()
		messageHandler, err := senderSocket.AcceptNewConnections()
		if err != nil {
			log.Error(getLogMessage("error accepting new connections in sendResponses", err))
			clientsSemaphore.Release()
			continue
		}

		go func(messageHandler client.MessageHandler) {
			defer clientsSemaphore.Release()
			defer closeService(messageHandler)

			err = s.handleSendResponses(messageHandler, dataQuery)
			if err != nil {
				log.Error(getLogMessage("error handling data from client", err))
				panic(err)
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
			return err
		}

		message := string(bodyBytes)

		// Receive EOF
		if s.isEOFMessage(message) {
			finMessage := utils.GetFINMessage(message)
			log.Debug(getLogMessage("receive FIN Message: "+finMessage, nil))

			if finMessage == s.config.FinishProcessingMessage {
				err = s.sendACK(messageHandler)
				if err != nil {
					log.Error(getLogMessage("Error sending finish processing ACK", err))
					return err
				}

				return nil
			}

			messageMetadata := utils.GetMetadataFromMessage(message)
			eofIdempotencyKey := fmt.Sprintf("%s-%s-%s", messageMetadata.City, messageMetadata.DataType, messageMetadata.ClientID)

			metadataEOF := dtos.NewMetadata(
				eofIdempotencyKey,
				true,
				messageMetadata.City,
				messageMetadata.DataType,
				serviceName,
				"message of the eof",
			)

			eofData := dtos.EOFData{
				Metadata: metadataEOF,
			}

			log.Debugf("EOF a mandar: %+v", eofData)

			eofDataBytes, err := json.Marshal(eofData)
			if err != nil {
				log.Error(getLogMessage("error marshaling EOF", err))
				return err
			}

			log.Debugf("Publish config: %+v", publishingConfig)
			err = eofStarter.Publish(publishingConfig.Exchange, eofDataBytes, publishingConfig.RoutingKey, publishingConfig.Topic)
			if err != nil {
				log.Error(getLogMessage("error publishing EOF", err))
				panic(err)
			}

			// Send ACK to client
			err = s.sendACK(messageHandler)
			if err != nil {
				log.Error(getLogMessage("error sending ACK of EOF", err))
				return err
			}

			continue
		}

		// Receive data with the following format data1|data2|...|dataN, where data is: clientID,batchNum,smsNum,dataType,city,data1,data2,...,dataN
		messageMetadata := utils.GetMetadataFromMessage(message)
		idempotencyKey := getIdempotencyKey(
			messageMetadata.ClientID,
			messageMetadata.BatchNumber,
			messageMetadata.City,
		)

		metadata := dtos.NewMetadata(
			idempotencyKey,
			false,
			messageMetadata.City,
			messageMetadata.DataType,
			serviceName,
			"metadata of the raw message",
		)

		messageSplit := strings.Split(message, s.config.DataDelimiter)

		dataToSend := dataentities.DataToSend{
			Metadata: metadata,
			Data:     messageSplit,
		}

		err = distributorQueue.SendMessage(dataToSend, "")
		if err != nil {
			log.Errorf(getLogMessage("error sending data to distributor", err))
			return err
		}

		err = s.sendACK(messageHandler)
		if err != nil {
			log.Error(getLogMessage("error sending ACK to client", err))
			return err
		}
	}
}

// handleSendResponses handles the request of the client and sends the response to te queries once it gets all the answers
func (s *Server) handleSendResponses(messageHandler client.MessageHandler, dataQuery dataentities.DataQuery) error {
	for {
		clientRequestBytes, err := messageHandler.Listen() // clientID-getResults
		if err != nil {
			log.Error(getLogMessage("error receiving data", err))
			return err
		}

		clientRequest := strings.Split(string(clientRequestBytes), "-")
		clientID := clientRequest[0]
		action := clientRequest[1]

		if action != "getResults" {
			return fmt.Errorf("unexpected action. Expected: getResults, got: %s", action)
		}

		queryData, ok := dataQuery.GetQueryValue(clientID)
		if !ok {
			err = messageHandler.Send([]byte("KEEP-ASKING")) // ToDo: add value in config file
			if err != nil {
				log.Error(getLogMessage("error sending keep asking message", err))
				return err
			}
			continue

		}

		dataMap, err := json.Marshal(queryData)
		if err != nil {
			log.Error(getLogMessage("error marshalling query response", err))
			return err
		}

		err = messageHandler.Send(dataMap)
		if err != nil {
			log.Error(getLogMessage("error sending query response", err))
			return err
		}

		log.Infof("Response sent to client with ID %s", clientID)
		return nil
	}
}

// sendACK sends an ACK though the given message handler
func (s *Server) sendACK(messageHandler client.MessageHandler) error {
	ack := []byte(s.config.ACKMessage)
	err := messageHandler.Send(ack)
	if err != nil {
		return fmt.Errorf("error sending ACK: %v", err)
	}

	return nil
}

func (s *Server) isEOFMessage(message string) bool {
	for _, eofMessage := range s.config.InputDataFinMessages {
		if strings.Contains(message, eofMessage) {
			return true
		}
	}
	return false
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

// getIdempotencyKey returns the idempotency key based on the given parameters
func getIdempotencyKey(clientID string, batchNumber string, city string) string {
	return fmt.Sprintf("%s-%s-%s-%s", clientID, batchNumber, city, clientID)
}
