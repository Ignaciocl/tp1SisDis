package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"os"
	"server/domain"
	"server/internal/config"
)

const (
	logLevelEnv     = "LOG_LEVEL"
	defaultLogLevel = "DEBUG"
	serviceName     = "server"
)

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
	logLevel := os.Getenv(logLevelEnv)
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	if err := InitLogger(logLevel); err != nil {
		fmt.Print(getLogMessage("error initializing logger", err))
		return
	}

	serverConfig, err := config.LoadConfig()
	if err != nil {
		log.Error(getLogMessage("error loading server config", err))
		return
	}

	server := domain.NewServer(serverConfig)
	log.Info(getLogMessage("Server initialized correctly, starting main functions...", nil))
	err = server.Run()

	log.Error(getLogMessage("error checking services", err))
	return
}

func getLogMessage(message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[main][service: %s][status: ERROR] %s: %v", serviceName, message, err)
	}

	return fmt.Sprintf("[main][service: %s][status: OK] %s", serviceName, message)
}
