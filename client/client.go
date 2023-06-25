package main

import (
	"bufio"
	"fmt"
	commons "github.com/Ignaciocl/tp1SisdisCommons/client"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"
)

const (
	fileFormat   = "csv"
	clientStr    = "client"
	tripsFile    = "trips"
	weatherFile  = "weather"
	stationsFile = "stations"
	queryStr     = "query"
)

var (
	cities       = []string{"montreal", "washington", "toronto"} // Debugging cities to sent: "washington", "toronto", "montreal"
	errorMessage = "error sending %s data from %s: %s"
)

type ClientConfig struct {
	BatchSize             int               `yaml:"batch_size"`
	FinMarker             string            `yaml:"fin_marker"`
	FinACKMessages        map[string]string `yaml:"fin_ack_messages"`
	ServerACK             string            `yaml:"server_ack"`
	ServerAddress         string            `yaml:"server_address"`
	ServerResponseAddress string            `yaml:"server_response_address"`
	PacketLimit           int               `yaml:"packet_limit"`
	CSVDelimiter          string            `yaml:"csv_delimiter"`
	DataDelimiter         string            `yaml:"data_delimiter"`
	EndBatchMarker        string            `yaml:"end_batch_marker"`
	Protocol              string            `yaml:"protocol"`
	GetResponsesMessage   string            `yaml:"get_response_message"`
	MaxConnectionRetries  int               `yaml:"max_connection_retries"`
	RetryDelay            time.Duration     `yaml:"retry_delay"`
	TestMode              bool
}

type Client struct {
	ID             string
	messageCounter int
	config         ClientConfig
	senderSocket   commons.Client
	receiverSocket commons.Client
}

func NewClient(id string, clientConfig ClientConfig) *Client {
	senderSocketConfig := commons.NewSocketConfig(
		clientConfig.Protocol,
		clientConfig.ServerAddress,
		clientConfig.PacketLimit,
	)

	receiverSocketConfig := commons.NewSocketConfig(
		clientConfig.Protocol,
		clientConfig.ServerResponseAddress,
		clientConfig.PacketLimit,
	)

	senderSocket := commons.NewSocket(senderSocketConfig)
	receiverSocket := commons.NewSocket(receiverSocketConfig)

	return &Client{
		ID:             id,
		config:         clientConfig,
		senderSocket:   senderSocket,
		receiverSocket: receiverSocket,
	}
}

// EstablishSenderConnection creates a TCP connection with the server to send the data from files
func (c *Client) EstablishSenderConnection() error {
	err := c.senderSocket.OpenConnection()
	if err != nil {
		return err
	}
	return nil
}

// EstablishReceiverConnection creates a TCP connection with the server to receive a response of the query
func (c *Client) EstablishReceiverConnection() error {
	err := c.receiverSocket.OpenConnection()
	if err != nil {
		return err
	}
	return nil
}

// Close closes the TCP connections with the server
func (c *Client) Close() {
	err := c.senderSocket.Close()
	if err != nil {
		log.Errorf("error closing sender connection: %v", err)
	}

	err = c.receiverSocket.Close()
	if err != nil {
		log.Errorf("error closing receiver connection: %v", err)
	}
}

// SendData sends the data of all the files from this client. The order is: weather, stations and trips
func (c *Client) SendData() error {
	log.Infof("Start sending data from client %s", c.ID)
	err := c.sendWeatherData()
	if err != nil {
		return err
	}

	err = c.sendStationsData()
	if err != nil {
		return err
	}

	err = c.sendTripsData()
	if err != nil {
		return err
	}

	log.Infof("All data from client %s was sent correctly", c.ID)
	return nil
}

// sendWeathersData sends all the data about weathers from this client
func (c *Client) sendWeatherData() error {
	for _, city := range cities {
		weatherFilepath := c.getFilePath(city, weatherFile)
		err := c.sendDataFromFile(weatherFilepath, city, weatherFile)
		if err != nil {
			log.Error(fmt.Sprintf(errorMessage, weatherFile, city, err.Error()))
			return err
		}
	}

	err := c.sendFinMessage(weatherFile)
	if err != nil {
		log.Errorf("[method:SendWeatherData]error sending FIN message: %s", err.Error())
		return err
	}

	return nil
}

// sendStationsData sends all the data about stations from this client
func (c *Client) sendStationsData() error {
	for _, city := range cities {
		stationsFilepath := c.getFilePath(city, stationsFile)
		err := c.sendDataFromFile(stationsFilepath, city, stationsFile)
		if err != nil {
			log.Error(fmt.Sprintf(errorMessage, stationsFile, city, err.Error()))
			return err
		}
	}

	err := c.sendFinMessage(stationsFile)
	if err != nil {
		log.Errorf("[method:SendStationsData]error sending FIN message: %s", err.Error())
		return err
	}

	return nil
}

// sendTripsData sends all the data about trips from this client
func (c *Client) sendTripsData() error {
	for _, city := range cities {
		tripsFilepath := c.getFilePath(city, tripsFile)
		err := c.sendDataFromFile(tripsFilepath, city, tripsFile)
		if err != nil {
			log.Error(fmt.Sprintf(errorMessage, tripsFile, city, err.Error()))
			return err
		}
	}

	err := c.sendFinMessage(tripsFile)
	if err != nil {
		log.Errorf("[method:SendTripsData]error sending FIN message: %s", err.Error())
		return err
	}

	return nil
}

// GetResponses asks for the response of the queries
func (c *Client) GetResponses() error {
	var responseStr string

	finMessageMarker := c.config.FinACKMessages[queryStr]

	messageBytes := []byte(c.config.GetResponsesMessage)
	for {
		err := c.receiverSocket.Send(messageBytes)

		if errors.Is(err, io.EOF) {
			err = c.reconnectReceiver()
			if err != nil {
				return err
			}
		}

		if err != nil && !errors.Is(err, io.EOF) {
			log.Errorf("[GetResponses] error sending Get response message: %v", err)
			continue
		}

		response, err := c.receiverSocket.Listen()

		if errors.Is(err, io.EOF) {
			err = c.reconnectReceiver()
			if err != nil {
				return err
			}
		}

		if err != nil && !errors.Is(err, io.EOF) {
			log.Errorf("error waiting for server response to queries: %v", err)
			continue
		}

		responseStr = string(response)
		if strings.Contains(responseStr, finMessageMarker) {
			break
		}

		log.Debugf("Keep waiting")
		randomNum := rand.Intn(14) + 1
		time.Sleep(time.Duration(randomNum) * time.Second)
	}

	log.Infof(responseStr)
	return nil
}

// sendDataFromFile reads the file from the given filepath and sends all its data
func (c *Client) sendDataFromFile(filepath string, city string, data string) error {
	dataFile, err := os.Open(filepath)
	if err != nil {
		log.Debugf("[clientID: %s][city: %s][data: %s] error opening %s: %s", c.ID, city, data, filepath, err.Error())
		return err
	}

	defer func(dataFile *os.File) {
		err := dataFile.Close()
		if err != nil {
			log.Errorf("error closing %s: %s", filepath, err.Error())
		}
	}(dataFile)

	fileScanner := bufio.NewScanner(dataFile)
	fileScanner.Split(bufio.ScanLines)
	_ = fileScanner.Scan() // Dismiss first line of the csv

	batchDataCounter := 0 // to know if the batch is full or not
	batchesSent := 0      // for debugging

	var dataToSend []string
	for fileScanner.Scan() {
		if batchDataCounter == c.config.BatchSize {
			batchesSent += 1
			log.Debugf("[clientID: %s][city: %s][data: %s] Sending batch number %v", c.ID, city, data, batchesSent)
			err = c.sendBatch(dataToSend)
			if err != nil {
				log.Errorf("[clientID: %s][city: %s][data: %s] error sending batch number %v: %s", c.ID, city, data, batchesSent, err.Error())
				return err
			}

			batchDataCounter = 0
			dataToSend = []string{}
		}
		line := fileScanner.Text()

		if len(line) < 2 {
			// sanity check
			break
		}

		c.messageCounter += 1
		lineTransformed := c.transformDataToSend(data, city, line)
		dataToSend = append(dataToSend, lineTransformed)
		batchDataCounter += 1
	}

	if len(dataToSend) != 0 {
		log.Debugf("[clientID: %s][city: %s][data: %s] Sending batch number %v", c.ID, city, data, batchesSent+1)
		err = c.sendBatch(dataToSend)
		if err != nil {
			log.Errorf("[clientID: %s][city: %s][data: %s] error sending batch number %v: %s", c.ID, city, data, batchesSent, err.Error())
			return err
		}
	}

	log.Debugf("[clientID: %s][city: %s][data: %s] All data was sent!", c.ID, city, data)
	return nil
}

// sendFinMessage sends a message to the server indicating that all the data from 'dataType' file was sent.
// + dataType possible values: weather, stations, trips
func (c *Client) sendFinMessage(dataType string) error {
	finMessage, ok := c.config.FinACKMessages[dataType]
	if !ok {
		panic(fmt.Sprintf("cannot find FIN-ACK message for data type %s", dataType))
	}
	log.Debugf("[data: %s] sending FIN MESSAGE %s", dataType, finMessage)
	finMessageBytes := []byte(finMessage)
	for {
		err := c.senderSocket.Send(finMessageBytes)

		if errors.Is(err, io.EOF) {
			err = c.reconnectSender()
			if err != nil {
				return err
			}
		}

		if err != nil && !errors.Is(err, io.EOF) {
			log.Errorf("error sending fin message %s: %v", finMessage, err)
			continue
		}

		log.Debugf("[data sent: %s] waiting for server response to FIN MESSAGE %s", dataType, finMessage)
		response, err := c.senderSocket.Listen()

		if errors.Is(err, io.EOF) {
			err = c.reconnectSender()
			if err != nil {
				return err
			}
		}

		if err != nil && !errors.Is(err, io.EOF) {
			log.Errorf("error waiting for server response to FIN message %s: %v", finMessage, err)
			continue
		}

		if string(response) != finMessage {
			log.Errorf("error got unexpected response. Expected %s, got: %s", finMessage, string(response))
			continue
		}

		log.Debugf("[clientID: %s][data sent: %s] Got server response %s", c.ID, dataType, string(response))
		return nil
	}
}

// reconnectSender reconnects the sender socket to the server
func (c *Client) reconnectSender() error {
	log.Infof("connection with server lost. Reconnecting sender...")
	retriesCounter := 0
	for {
		if retriesCounter == c.config.MaxConnectionRetries {
			return fmt.Errorf("error max retries attempts reached, cannot connect sender to server")
		}

		err := c.senderSocket.OpenConnection()
		if err != nil {
			retriesCounter += 1
			time.Sleep(c.config.RetryDelay)
			continue
		}

		log.Infof("Sender connected again!")
		return nil
	}
}

// reconnectReceiver reconnects the receiver socket to the server
func (c *Client) reconnectReceiver() error {
	log.Infof("connection with server lost. Reconnecting receiver...")
	retriesCounter := 0
	for {
		if retriesCounter == c.config.MaxConnectionRetries {
			return fmt.Errorf("error max retries attempts reached, cannot connect receiver to server")
		}

		err := c.receiverSocket.OpenConnection()
		if err != nil {
			retriesCounter += 1
			time.Sleep(c.config.RetryDelay)
			continue
		}

		log.Infof("Receiver connected again!")
		return nil
	}
}

// sendBatch sends a batch with data to the server and waits for its ACK
func (c *Client) sendBatch(batch []string) error {
	debugCity := strings.SplitN(batch[0], ",", 6)[3]

	// Join data with |, e.g data1|data2|data3|..., eg of data: clientID,messageNum,dataType,city,weatherField1,weatherField2,...
	dataJoined := strings.Join(batch, c.config.DataDelimiter)
	dataJoined = dataJoined + c.config.DataDelimiter + c.config.EndBatchMarker // the message to send has the following format: data1|data2|data3|...|dataN|PING
	dataJoinedBytes := []byte(dataJoined)

	for {
		err := c.senderSocket.Send(dataJoinedBytes)

		if errors.Is(err, io.EOF) {
			err = c.reconnectSender()
			if err != nil {
				return err
			}
		}

		if err != nil && !errors.Is(err, io.EOF) {
			log.Errorf("[city: %s] error sending batch: %v", debugCity, err)
			continue
		}

		log.Debugf("[city: %s] data sent, waiting for server ACK", debugCity)
		response, err := c.senderSocket.Listen()

		if errors.Is(err, io.EOF) {
			err = c.reconnectSender()
			if err != nil {
				return err
			}
		}

		if err != nil {
			log.Debugf("[city: %s] error while wainting for server ACK: %s", debugCity, err.Error())
			return err
		}

		if string(response) != c.config.ServerACK {
			log.Errorf("error got unexpected response. Expected %s, got: %s. Retrying...", c.config.ServerACK, string(response))
			continue
		}

		return nil
	}
}

// getFilePath returns the path to the .csv file.
// + City possible values: montreal, toronto or washington
// + Filename possible values: weather, stations, trips
func (c *Client) getFilePath(city string, filename string) string {
	if c.config.TestMode {
		return fmt.Sprintf("./datasets/test/%s/%s/%s_test.%s", clientStr+c.ID, city, filename, fileFormat)
	}
	return fmt.Sprintf("./datasets/%s/%s/%s.%s", clientStr+c.ID, city, filename, fileFormat)
}

// transformDataToSend transforms the data from the file adding the following info:
// + ID: id of the client
// + Message number: the number of the message
// + Data type: could be weather, trips or stations
// + City: city which this data belongs
func (c *Client) transformDataToSend(dataType string, city string, originalData string) string {
	extraData := fmt.Sprintf("%s,%v,%s,%s", c.ID, c.messageCounter, dataType, city)
	return extraData + c.config.CSVDelimiter + originalData
}
