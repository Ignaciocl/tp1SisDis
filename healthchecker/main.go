package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"tp1SisDis/healthchecker/config"
	"tp1SisDis/healthchecker/entity"
)

const (
	logLevelEnv     = "LOG_LEVEL"
	defaultLogLevel = "INFO"
	service         = "health-checker"
)

// initLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func initLogger(logLevel string) error {
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
	logLevel := os.Getenv(logLevelEnv)
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	if err := initLogger(logLevel); err != nil {
		fmt.Print(getLogMessage("error initializing logger", err))
		return
	}

	healthCheckerConfig, err := config.LoadConfig()
	if err != nil {
		log.Error(getLogMessage("error loading health checker config", err))
		return
	}

	healthChecker := entity.NewHealthChecker(healthCheckerConfig)
	log.Info(getLogMessage("Health checker initialized correctly, beginning to checking services...", nil))
	err = healthChecker.Run()

	log.Error(getLogMessage("error checking services", err))
	return
}

func getLogMessage(message string, err error) string {
	// ToDo: put the correct ID
	if err != nil {
		return fmt.Sprintf("[service: %s][ID: %v][status: ERROR] %s: %v", service, 1, message, err)
	}
	return fmt.Sprintf("[service: %s][ID: %v][status: OK] %s", service, 1, message)
}
