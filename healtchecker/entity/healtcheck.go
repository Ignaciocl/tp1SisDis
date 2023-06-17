package entity

import (
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons/client"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
	"tp1SisDis/healthchecker/config"
)

type HealthChecker struct {
	config *config.HealthCheckerConfig
}

func NewHealthChecker(cfg *config.HealthCheckerConfig) *HealthChecker {
	return &HealthChecker{
		config: cfg,
	}
}

func (hc *HealthChecker) Run() error {
	errorChannel := make(chan error)
	var waitGroup sync.WaitGroup
	for _, serviceName := range hc.config.Services {
		socket := common.NewSocket(common.SocketConfig{ // ToDo: add constructor in commons
			Protocol:    hc.config.Protocol,
			NodeAddress: fmt.Sprintf("%s:%v", serviceName, hc.config.Port),
			NodeACK:     hc.config.ExpectedResponse, // ToDo: check if we need this param
			PacketLimit: hc.config.PacketLimit,
		})

		err := socket.OpenConnection()
		if err != nil {
			return err
		}

		waitGroup.Add(1)
		go func(socket common.Client, channel chan error, service string) {
			defer waitGroup.Done()
			hc.checkServiceStatus(socket, errorChannel, service)

		}(socket, errorChannel, serviceName)

	}

	err := <-errorChannel
	close(errorChannel)

	waitGroup.Wait()
	return err
}

// checkServiceStatus sends a heartbeat to a given service. Each heartbeat is sent with a fixed frequency.
// In case of errors, it retries some fixed amount of times, once this threshold is passed, an error is returned.
func (hc *HealthChecker) checkServiceStatus(socket common.Client, errorChannel chan error, serviceName string) {
	intervalTicker := time.NewTicker(hc.config.Interval)
	var retryTicker *time.Ticker
	retriesCounter := 0

	defer intervalTicker.Stop()
	defer retryTicker.Stop()

	heartbeatBytes := []byte(hc.config.Message)

	for {
		if retriesCounter == hc.config.MaxRetries {
			//errorChannel <- fmt.Errorf("%w: %s does not respond", errServiceUnhealthy, serviceName)
			log.Errorf("%s: %s does not respond", errServiceUnhealthy, serviceName)
			// ToDo: we should kill the service or bring them to life again (Bring me to life - Evanescence.mp3)
			return
		}
		select {
		case <-intervalTicker.C:
			// Send a new heartbeat
			retriesCounter = 0
			err := socket.Send(heartbeatBytes)
			if err != nil {
				retryTicker = time.NewTicker(hc.config.RetryDelay)
				continue
			}

			response, err := socket.Listen()
			if err != nil {
				retryTicker = time.NewTicker(hc.config.RetryDelay)
				continue
			}

			if string(response) != hc.config.ExpectedResponse {
				retryTicker = time.NewTicker(hc.config.RetryDelay)
				continue
			}

		case <-retryTicker.C:
			retriesCounter += 1
			err := socket.Send(heartbeatBytes)
			if err != nil {
				continue
			}

			response, err := socket.Listen()
			if err != nil {
				continue
			}

			if string(response) != hc.config.ExpectedResponse {
				continue
			}

			retriesCounter = 0
			retryTicker.Stop()
		}
	}
}
