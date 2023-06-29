package main

import (
	"bufio"
	"fmt"
	commons "github.com/Ignaciocl/tp1SisdisCommons/client"
	log "github.com/sirupsen/logrus"
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
	finishKey    = "finish"
	wildcardCity = "*"
)

var (
	cities       = []string{"montreal", "toronto", "washington"} // Debugging cities to sent: "washington", "toronto", "montreal"
	errorMessage = "error sending %s data from %s: %s"
)

type ClientConfig struct {
	Protocol              string        `yaml:"protocol"`
	ServerAddress         string        `yaml:"server_address"`
	ServerResponseAddress string        `yaml:"server_response_address"`
	PacketLimit           int           `yaml:"packet_limit"`
	MaxConnectionRetries  int           `yaml:"max_connection_retries"`
	RetryDelay            time.Duration `yaml:"retry_delay"`

	BatchSize     int    `yaml:"batch_size"`
	ServerACK     string `yaml:"server_ack"`
	CSVDelimiter  string `yaml:"csv_delimiter"`
	DataDelimiter string `yaml:"data_delimiter"`

	FinACKMessages      map[string]string `yaml:"fin_ack_messages"`
	QueryResultsMessage string            `yaml:"query_results_message"`
	KeepAskingResponse  string            `yaml:"keep_asking_response"`
	TestMode            bool
}

type Client struct {
	ID             string
	messageCounter int
	batchCounter   int
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

		log.Infof("Sender connected!")
		return nil
	}
}

// EstablishReceiverConnection creates a TCP connection with the server to receive a response of the query
func (c *Client) EstablishReceiverConnection() error {
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

		log.Infof("Receiver connected!")
		return nil
	}
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
	for _, city := range cities {
		err := c.sendStationsData(city)
		if err != nil {
			return err
		}

		err = c.sendWeatherData(city)
		if err != nil {
			return err
		}

		err = c.sendTripsData(city)
		if err != nil {
			return err
		}
	}

	err := c.sendFinMessage(finishKey, wildcardCity) // We don't need the city, it's just a FIN connection message
	if err != nil {
		return err
	}

	log.Infof("All data from client %s was sent correctly", c.ID)
	return nil
}

// sendWeathersData sends all the data about weathers from this client
func (c *Client) sendWeatherData(city string) error {
	weatherFilepath := c.getFilePath(city, weatherFile)
	err := c.sendDataFromFile(weatherFilepath, city, weatherFile)
	if err != nil {
		log.Error(fmt.Sprintf(errorMessage, weatherFile, city, err.Error()))
		return err
	}

	err = c.sendFinMessage(weatherFile, city)
	if err != nil {
		log.Errorf("[method:SendWeatherData]error sending FIN message: %s", err.Error())
		return err
	}

	log.Info("All Weather data was sent!")
	return nil
}

// sendStationsData sends all the data about stations from this client
func (c *Client) sendStationsData(city string) error {
	stationsFilepath := c.getFilePath(city, stationsFile)
	err := c.sendDataFromFile(stationsFilepath, city, stationsFile)
	if err != nil {
		log.Error(fmt.Sprintf(errorMessage, stationsFile, city, err.Error()))
		return err
	}

	err = c.sendFinMessage(stationsFile, city)
	if err != nil {
		log.Errorf("[method:SendStationsData]error sending FIN message: %s", err.Error())
		return err
	}

	log.Info("All Stations data was sent!")
	return nil
}

// sendTripsData sends all the data about trips from this client
func (c *Client) sendTripsData(city string) error {
	tripsFilepath := c.getFilePath(city, tripsFile)
	err := c.sendDataFromFile(tripsFilepath, city, tripsFile)
	if err != nil {
		log.Error(fmt.Sprintf(errorMessage, tripsFile, city, err.Error()))
		return err
	}

	err = c.sendFinMessage(tripsFile, city)
	if err != nil {
		log.Errorf("[method:SendTripsData]error sending FIN message: %s", err.Error())
		return err
	}

	log.Info("All Trips data was sent!")
	return nil
}

// GetResponses asks for the response of the queries
func (c *Client) GetResponses() error {
	var responseStr string
	getResultsMessage := fmt.Sprintf("%s-%s", c.ID, c.config.QueryResultsMessage)

	messageBytes := []byte(getResultsMessage)
	for {
		err := c.receiverSocket.Send(messageBytes)

		if err != nil {
			log.Infof("connection with server lost. Reconnecting receiver...")
			err = c.EstablishReceiverConnection()
			if err != nil {
				return err
			}
			continue
		}

		/*if err != nil && !errors.Is(err, io.EOF) {
			log.Errorf("[GetResponses] error sending Get response message: %v", err)
			continue
		}*/

		response, err := c.receiverSocket.Listen()

		if err != nil {
			log.Infof("connection with server lost. Reconnecting receiver...")
			err = c.EstablishReceiverConnection()
			if err != nil {
				return err
			}
			continue
		}

		/*if err != nil && !errors.Is(err, io.EOF) {
			log.Errorf("error waiting for server response to queries: %v", err)
			continue
		}*/

		responseStr = string(response)
		if responseStr != c.config.KeepAskingResponse {
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

	var dataToSend []string
	c.batchCounter += 1
	for fileScanner.Scan() {
		if batchDataCounter == c.config.BatchSize {
			c.batchCounter += 1
			log.Debugf("[clientID: %s][city: %s][data: %s] Sending batch number %v", c.ID, city, data, c.batchCounter)
			err = c.sendBatch(dataToSend)
			if err != nil {
				log.Errorf("[clientID: %s][city: %s][data: %s] error sending batch number %v: %s", c.ID, city, data, c.batchCounter, err.Error())
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
		c.batchCounter += 1
		log.Debugf("[clientID: %s][city: %s][data: %s] Sending batch number %v", c.ID, city, data, c.batchCounter)
		err = c.sendBatch(dataToSend)
		if err != nil {
			log.Errorf("[clientID: %s][city: %s][data: %s] error sending batch number %v: %s", c.ID, city, data, c.batchCounter, err.Error())
			return err
		}
	}

	log.Debugf("[clientID: %s][city: %s][data: %s] All data was sent!", c.ID, city, data)
	return nil
}

// sendFinMessage sends a message to the server indicating that all the data from 'dataType' file was sent.
// + dataType possible values: weather, stations, trips
// + city: montreal, washington or toronto
func (c *Client) sendFinMessage(dataType string, city string) error {
	finMessage, ok := c.config.FinACKMessages[dataType]
	if !ok {
		panic(fmt.Sprintf("cannot find FIN-ACK message for data type %s", dataType))
	}

	finMessage = c.transformDataToSend(dataType, city, finMessage)
	log.Debugf("[data: %s] sending FIN MESSAGE %s", dataType, finMessage)
	finMessageBytes := []byte(finMessage)

	for {
		err := c.senderSocket.Send(finMessageBytes)

		if err != nil {
			log.Infof("connection with server lost. Reconnecting sender...")
			err = c.EstablishSenderConnection()
			if err != nil {
				return err
			}
			continue
		}

		log.Debugf("[data sent: %s] waiting for server response to FIN MESSAGE %s", dataType, finMessage)
		response, err := c.senderSocket.Listen()

		if err != nil {
			log.Infof("connection with server lost. Reconnecting sender...")
			err = c.EstablishSenderConnection()
			if err != nil {
				return err
			}
			continue
		}

		if string(response) != c.config.ServerACK {
			log.Errorf("error got unexpected response. Expected %s, got: %s", finMessage, string(response))
			continue
		}

		log.Debugf("[clientID: %s][data sent: %s] Got server response %s", c.ID, dataType, string(response))
		return nil
	}
}

// sendBatch sends a batch with data to the server and waits for its ACK
func (c *Client) sendBatch(batch []string) error {
	debugCity := strings.SplitN(batch[0], ",", 6)[4]

	// Join data with |, e.g data1|data2|data3|...|dataN eg of data: clientID,messageNum,dataType,city,weatherField1,weatherField2,...
	dataJoined := strings.Join(batch, c.config.DataDelimiter)
	dataJoinedBytes := []byte(dataJoined)

	for {
		err := c.senderSocket.Send(dataJoinedBytes)

		if err != nil {
			log.Infof("connection with server lost. Reconnecting sender...")
			err = c.EstablishSenderConnection()
			if err != nil {
				return err
			}
			continue
		}

		log.Debugf("[city: %s] data sent, waiting for server ACK", debugCity)
		response, err := c.senderSocket.Listen()

		if err != nil {
			log.Infof("connection with server lost. Reconnecting sender...")
			err = c.EstablishSenderConnection()
			if err != nil {
				return err
			}
			continue
		}

		if string(response) != c.config.ServerACK {
			log.Errorf("error got unexpected response. Expected %s, got: %s. Retrying...", c.config.ServerACK, string(response))
			continue
		}

		log.Debugf("[city: %s] receive ACK of batch: %s", debugCity, response)
		return nil
	}
}

// getFilePath returns the path to the .csv file.
// + City possible values: montreal, toronto or washington
// + Filename possible values: weather, stations, trips
func (c *Client) getFilePath(city string, filename string) string {
	if c.config.TestMode {
		return fmt.Sprintf("../datasets/test/client_%s/%s/%s.%s", c.ID, city, filename, fileFormat)
	}
	return fmt.Sprintf("./datasets/client_%s/%s/%s.%s", c.ID, city, filename, fileFormat)
}

// transformDataToSend transforms the data from the file adding the following info:
// + ID: id of the client
// + Batch number: number of the batch sent
// + Message number: the number of the message
// + Data type: could be weather, trips or stations
// + City: city which this data belongs
func (c *Client) transformDataToSend(dataType string, city string, originalData string) string {
	extraData := fmt.Sprintf("%s,%v,%v,%s,%s", c.ID, c.batchCounter, c.messageCounter, dataType, city)
	return extraData + c.config.CSVDelimiter + originalData
}
