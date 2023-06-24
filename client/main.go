package main

import (
	"fmt"
	"github.com/Ignaciocl/tp1SisdisCommons/configloader"
	"gopkg.in/yaml.v3"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const (
	testEnvVar = "TEST_MODE"
	filepath   = "./config/config.yaml"
)

var dataTypes = []string{"weather", "stations", "trips"} // Debugging data to sent: "weather", "stations", "trips"

func LoadClientConfig() (ClientConfig, error) {
	configFile, err := configloader.GetConfigFileAsBytes(filepath)
	if err != nil {
		return ClientConfig{}, err
	}

	var clientConfig ClientConfig
	err = yaml.Unmarshal(configFile, &clientConfig)
	if err != nil {
		return ClientConfig{}, fmt.Errorf("error parsing client config file: %s", err)
	}

	testMode := os.Getenv(testEnvVar)
	clientConfig.TestMode = testMode == "true"

	return clientConfig, nil
}

// InitLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	customFormatter := &logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   false,
	}
	logrus.SetFormatter(customFormatter)
	logrus.SetLevel(level)
	return nil
}

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "DEBUG"
	}
	if err := InitLogger(logLevel); err != nil {
		log.Fatalf("%s", err)
	}

	clientConfig, err := LoadClientConfig()
	if err != nil {
		logrus.Errorf(err.Error())
		return
	}

	log.Debugf("client config: %+v", clientConfig)

	var wg sync.WaitGroup
	for _, data := range dataTypes {
		wg.Add(1)
		client := NewClient(clientConfig)

		go func(data string) {
			defer wg.Done()
			err := sendData(client, data)
			if err != nil {
				fmt.Printf("error sendind %s data from cliente: %s", data, err.Error())
			}
		}(data)
	}

	log.Info("[client] Waiting for threads")
	wg.Wait()

	client := NewClient(clientConfig)
	err = client.OpenConnection(client.config.ServerResponseAddress)
	if err != nil {
		log.Errorf("Error opening connection for responses: %s", err.Error())
		return
	}

	err = client.GetResponses()
	if err != nil {
		log.Errorf("Error getting responses: %s", err.Error())
		return
	}
	log.Info("[client] Finish main.go")
}

func sendData(client *Client, data string) error {
	err := client.OpenConnection(client.config.ServerAddress)
	defer func(client *Client) {
		err := client.CloseConnection()
		if err != nil {
			log.Error("[client] error closing connection")
			return
		}
		log.Info("[client] connection closed successfully!")
	}(client)
	if err != nil {
		logrus.Errorf(err.Error())
		return err
	}

	if data == "weather" {
		err = client.SendWeatherData()
	}

	if data == "trips" {
		err = client.SendTripsData()
	}

	if data == "stations" {
		err = client.SendStationsData()
	}

	if err != nil {
		logrus.Errorf(err.Error())
		return err
	}

	return nil
}
