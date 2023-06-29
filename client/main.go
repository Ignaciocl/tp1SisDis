package main

import (
	"fmt"
	"github.com/Ignaciocl/tp1SisdisCommons/configloader"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"os"
)

const (
	testEnvVar      = "TEST_MODE"
	clientIDEnvVar  = "CLIENT_ID"
	logLevelEnvVar  = "LOG_LEVEL"
	defaultLogLevel = "DEBUG"
	filepath        = "./config/config.yaml"
)

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

	//testMode := os.Getenv(testEnvVar)
	clientConfig.TestMode = true

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
	logLevel := os.Getenv(logLevelEnvVar)
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	if err := InitLogger(logLevel); err != nil {
		fmt.Printf("error initializating logger: %v", err)
		os.Exit(1)
	}

	clientID := os.Getenv(clientIDEnvVar)
	if clientID == "" {
		fmt.Printf("error missing client ID")
		return
	}

	clientConfig, err := LoadClientConfig()
	if err != nil {
		logrus.Errorf(err.Error())
		return
	}

	log.Debugf("client config: %+v", clientConfig)
	client := NewClient(clientID, clientConfig)
	defer client.Close()

	err = client.EstablishSenderConnection()
	if err != nil {
		log.Errorf("error establishing connection with server to send data: %v", err)
		return
	}

	err = client.SendData()
	if err != nil {
		log.Errorf("error sending data to server: %v", err)
		return
	}

	err = client.EstablishReceiverConnection()
	if err != nil {
		log.Errorf("error establishing connection with server to receive responses: %v", err)
		return
	}

	err = client.GetResponses()
	if err != nil {
		log.Errorf("error receiveng responses from server: %v", err)
		return
	}

	log.Info("Well, that")
}
