package entity

import (
	"context"
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons/client"
	"github.com/docker/docker/api/types/container"
	dockerClient "github.com/docker/docker/client"
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
		socket := common.NewSocket(common.NewSocketConfig(
			hc.config.Protocol,
			fmt.Sprintf("%s:%v", serviceName, hc.config.Port),
			hc.config.PacketLimit,
		))

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
	retryTicker := time.NewTicker(hc.config.Interval)
	retryTicker.Stop()
	retriesCounter := 0

	defer intervalTicker.Stop()
	defer func() {
		if retryTicker != nil {
			retryTicker.Stop()
		}
	}()

	heartbeatBytes := []byte(hc.config.Message)

	for {
		if retriesCounter == hc.config.MaxRetries {
			retryTicker.Stop()
			_ = socket.Close()
			//errorChannel <- fmt.Errorf("%w: %s does not respond", errServiceUnhealthy, serviceName) // ToDo: talk with Nacho about using this channel in some cases
			log.Errorf("%s: %s does not respond", errServiceUnhealthy, serviceName)
			err := hc.hastaLaVistaBaby(serviceName)
			if err != nil {
				log.Errorf("%v", err)
				continue
			}

			err = hc.bringMeToLife(serviceName)
			if err != nil {
				log.Errorf("%v", err)
				continue
			}

			time.Sleep(hc.config.GraceTime)

			err = socket.OpenConnection()
			if err != nil {
				log.Errorf("%s: %v", errSettingConnection, err)
				continue
			}
			retriesCounter = 0
			intervalTicker.Reset(hc.config.Interval)
			continue
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

			log.Debugf("Lichita: recibi esta respuesta: %s", string(response))
			if string(response) != hc.config.ExpectedResponse {
				retryTicker = time.NewTicker(hc.config.RetryDelay)
				continue
			}

		case <-retryTicker.C:
			log.Debug("Some error occurs, trying again...")
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

// hastaLaVistaBaby stops a container
func (hc *HealthChecker) hastaLaVistaBaby(containerName string) error {
	dockerCli, err := getDockerClient()
	if err != nil {
		return err
	}

	err = dockerCli.ContainerStop(context.Background(), containerName, container.StopOptions{Timeout: &hc.config.TTL})
	if err != nil {
		return fmt.Errorf("%w: couldn't stop service '%s': %v", errKillingService, containerName, err)
	}

	log.Debugf("Container %s stopped successfully", containerName)
	return nil

}

// bringMeToLife brings back to life a container
// more info here: https://www.youtube.com/watch?v=3YxaaGgTQYM&ab_channel=EvanescenceVEVO
func (hc *HealthChecker) bringMeToLife(containerName string) error {
	dockerCli, err := getDockerClient()
	if err != nil {
		return err
	}

	err = dockerCli.ContainerRestart(context.Background(), containerName, container.StopOptions{})
	if err != nil {
		return fmt.Errorf("%w: couldn't bring back to life service '%s': %v", errBringingBackToLifeService, containerName, err)
	}

	log.Debugf("Container %s bringed back to life succesffully!", containerName)
	return nil
}

// getDockerClient returns a docker client initialized it
func getDockerClient() (dockerClient.APIClient, error) {
	cli, err := dockerClient.NewClientWithOpts(dockerClient.FromEnv, dockerClient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errDockerClient, err)
	}
	return cli, err
}
