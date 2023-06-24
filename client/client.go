package main

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"strings"
	"time"
	"tp1/socket"
)

const (
	fileFormat   = "csv"
	tripsFile    = "trips"
	weatherFile  = "weather"
	stationsFile = "stations"
)

var (
	cities       = []string{"montreal", "washington", "toronto"} // Debugging cities to sent: "washington", "toronto", "montreal"
	errorMessage = "error sending %s data from %s: %s"
)

type ClientConfig struct {
	BatchSize             int      `yaml:"batch_size"`
	FinMarker             string   `yaml:"fin_marker"`
	FinACKMessages        []string `yaml:"fin_ack_messages"`
	ServerACK             string   `yaml:"server_ack"`
	ServerAddress         string   `yaml:"server_address"`
	ServerResponseAddress string   `yaml:"server_response_address"`
	PacketLimit           int      `yaml:"packet_limit"`
	CSVDelimiter          string   `yaml:"csv_delimiter"`
	DataDelimiter         string   `yaml:"data_delimiter"`
	EndBatchMarker        string   `yaml:"end_batch_marker"`
	Protocol              string   `yaml:"protocol"`
	GetResponsesMessage   string   `yaml:"get_response_message"`
	TestMode              bool
}

type Client struct {
	config       ClientConfig
	clientSocket *socket.Socket
}

func NewClient(clientConfig ClientConfig) *Client {
	return &Client{
		config: clientConfig,
	}
}

// OpenConnection creates a TCP connection with the server
func (c *Client) OpenConnection(serverAddress string) error {
	clientSocket := socket.NewSocket(
		socket.SocketConfig{
			ServerAddress: serverAddress,
			ServerACK:     c.config.ServerACK,
			PacketLimit:   c.config.PacketLimit,
			Protocol:      c.config.Protocol,
		},
	)

	err := clientSocket.OpenConnection()
	if err != nil {
		return err
	}
	c.clientSocket = clientSocket
	return nil
}

// CloseConnection close the TCP connection with the server
func (c *Client) CloseConnection() error {
	err := c.clientSocket.CloseConnection()
	if err != nil {
		log.Errorf("error closing connection: %s", err.Error())
		return err
	}
	return nil
}

func (c *Client) SendWeatherData() error {
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

func (c *Client) SendStationsData() error {
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

func (c *Client) SendTripsData() error {
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

func (c *Client) GetResponses() error {
	var responseStr string
	for {
		err := c.clientSocket.Send(c.config.GetResponsesMessage)
		if err != nil {
			return fmt.Errorf("[GetResponses] error sending Get response message: %s", err.Error())
		}

		response, err := c.clientSocket.Listen("PONG", c.config.FinACKMessages)
		responseStr = string(response)
		if responseStr != c.config.GetResponsesMessage {
			break
		}
		log.Debugf("Keep waiting")
		randomNum := rand.Intn(14) + 1
		time.Sleep(time.Duration(randomNum) * time.Second)
	}
	log.Infof(responseStr)
	return nil
}

func (c *Client) sendDataFromFile(filepath string, city string, data string) error {
	dataFile, err := os.Open(filepath)
	if err != nil {
		log.Debugf("[city: %s][data: %s] error opening %s: %s", city, data, filepath, err.Error())
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

	dataCounter := 0
	batchesSent := 0
	var dataToSend []string
	for fileScanner.Scan() {
		if dataCounter == c.config.BatchSize {
			batchesSent += 1
			log.Debugf("[city: %s][data: %s] Sending batch number %v", city, data, batchesSent)
			err = c.sendBatch(dataToSend)
			if err != nil {
				log.Errorf("[city: %s][data: %s] error sending batch number %v: %s", city, data, batchesSent, err.Error())
				return err
			}
			dataCounter = 0
			dataToSend = []string{}
		}
		line := fileScanner.Text()
		if len(line) < 2 {
			// sanity check
			break
		}
		line = data + c.config.CSVDelimiter + city + c.config.CSVDelimiter + line //prepend data type and city to data
		dataToSend = append(dataToSend, line)
		dataCounter += 1
	}

	if len(dataToSend) != 0 {
		log.Debugf("[city: %s][data: %s] Sending batch number %v", city, data, batchesSent+1)
		err = c.sendBatch(dataToSend)
		if err != nil {
			log.Errorf("[city: %s][data: %s] error sending batch number %v: %s", city, data, batchesSent, err.Error())
			return err
		}
	}

	log.Debugf("[city: %s][data: %s]All data was sent!", city, data)
	return nil
}

// sendFinMessage sends a message to the server indicating that all the data from 'dataType' file was sent.
// + dataType possible values: weather, stations, trips
func (c *Client) sendFinMessage(dataType string) error {
	finMessage := dataType + "-" + c.config.FinMarker
	log.Debugf("[data: %s] sending FIN MESSAGE %s", dataType, finMessage)
	err := c.clientSocket.Send(finMessage)
	if err != nil {
		log.Errorf("error sending fin message %s: %s", finMessage, err.Error())
		return err
	}

	log.Debugf("[data sent: %s] waiting for server response to FIN MESSAGE %s", dataType, finMessage)
	response, err := c.clientSocket.Listen("", c.config.FinACKMessages)
	if err != nil {
		log.Errorf("error waiting for server response to fin message %s: %s", finMessage, err.Error())
		return err
	}

	log.Debugf("[client][data sent: %s] Got server response %s", dataType, string(response))
	return nil
}

// sendBatch sends a batch with data to the server and waits for its ACK
func (c *Client) sendBatch(batch []string) error {
	debugCity := strings.SplitN(batch[0], ",", 2)[0]

	// Join data with |, e.g data1|data2|data3|..., eg od data: dataType,city,weatherField1,weatherField2,...
	dataJoined := strings.Join(batch, c.config.DataDelimiter)
	dataJoined = dataJoined + c.config.DataDelimiter + c.config.EndBatchMarker // the message to send has the following format: data1|data2|data3|...|dataN|PING
	err := c.clientSocket.Send(dataJoined)
	if err != nil {
		return fmt.Errorf("[city: %s] error sending batch: %s", debugCity, err.Error())
	}

	log.Debugf("[city: %s] data sent, waiting for server ACK", debugCity)
	_, err = c.clientSocket.Listen(c.config.ServerACK, []string{})
	if err != nil {
		log.Debugf("[city: %s] error while wainting for server ACK: %s", debugCity, err.Error())
		return err
	}

	return nil
}

// getFilePath returns the path to the .csv file.
// + City possible values: montreal, toronto or washington
// + Filename possible values: weather, stations, trips
func (c *Client) getFilePath(city string, filename string) string {
	if c.config.TestMode {
		return fmt.Sprintf("./datasets/test/%s/%s_test.%s", city, filename, fileFormat)
	}
	return fmt.Sprintf("./datasets/%s/%s.%s", city, filename, fileFormat)
}
