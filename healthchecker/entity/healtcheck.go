package entity

import (
	"context"
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons/client"
	commonHealthcheck "github.com/Ignaciocl/tp1SisdisCommons/healthcheck"
	"github.com/Ignaciocl/tp1SisdisCommons/leader"
	"github.com/Ignaciocl/tp1SisdisCommons/utils"
	"github.com/docker/docker/api/types/container"
	dockerClient "github.com/docker/docker/client"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
	"tp1SisDis/healthchecker/config"
)

const healthCheckerServiceName = "diosito"

type HealthChecker struct {
	id     string
	config *config.HealthCheckerConfig
}

func NewHealthChecker(id string, cfg *config.HealthCheckerConfig) *HealthChecker {
	return &HealthChecker{
		id:     id,
		config: cfg,
	}
}

func (hc *HealthChecker) Run() error {
	errorChannel := make(chan error)
	var waitGroup sync.WaitGroup

	log.Debugf("Iniciando health checker: %s", healthCheckerServiceName+hc.id)
	healthCheckerReplier := commonHealthcheck.InitHealthCheckerReplier(healthCheckerServiceName + hc.id)

	go func() {
		err := healthCheckerReplier.Run()
		utils.FailOnError(err, "health checker replier error")
	}()

	ring := make(map[string]string, hc.config.AmountOfHealthCheckers)
	for healthCheckerID := 1; healthCheckerID <= hc.config.AmountOfHealthCheckers; healthCheckerID++ {
		ring[fmt.Sprintf("%v", healthCheckerID)] = fmt.Sprintf("%s%v:%v", healthCheckerServiceName, healthCheckerID, hc.config.ElectionPort)
	}

	// Create Bully Election Handler
	bully, err := leader.NewBully(
		hc.id,
		fmt.Sprintf("%s%s:%v", healthCheckerServiceName, hc.id, hc.config.ElectionPort),
		hc.config.Protocol,
		ring,
	)

	if err != nil {
		return err
	}

	defer bully.Close()

	go func() {
		bully.Run()
	}()

	bully.WakeMeUpWhenSeptemberEnds()
	for _, serviceName := range hc.config.Services {
		if serviceName == healthCheckerServiceName+hc.id {
			continue
		}

		socket := common.NewSocket(common.NewSocketConfig(
			hc.config.Protocol,
			fmt.Sprintf("%s:%v", serviceName, hc.config.Port),
			hc.config.PacketLimit,
		))

		waitGroup.Add(1)
		go func(socket common.Client, channel chan error, service string, electionHandler leader.Leader) {
			defer waitGroup.Done()
			defer log.Infof("VA A MORIR EL THREAD")
			hc.checkServiceStatus(socket, errorChannel, service, electionHandler)

		}(socket, errorChannel, serviceName, bully)
	}

	log.Infof("POR ALGUNA RAZON SALI DEL LOOP: %s", healthCheckerServiceName+hc.id)
	err = <-errorChannel
	close(errorChannel)

	waitGroup.Wait()
	return err
}

// checkServiceStatus sends a heartbeat to a given service. Each heartbeat is sent with a fixed frequency.
// In case of errors, it retries some fixed amount of times, once this threshold is passed, an error is returned.
func (hc *HealthChecker) checkServiceStatus(socket common.Client, errorChannel chan error, serviceName string, electionHandler leader.Leader) {
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

	//_ = setConnection(socket, hc.config.MaxConnectionRetries, hc.config.ConnectionRetryDelay) // we don't need to handle the error, if it can't connect it means it's death

	heartbeatBytes := []byte(hc.config.Message)

	for {
		if !electionHandler.IsLeader() {
			log.Infof("YA NO SOY LIDER DIOSITO ID %s", hc.id)

			// Stop tickers
			retryTicker.Stop()
			intervalTicker.Stop()

			// Close connections
			if socket.IsConnectionOpen() {
				_ = socket.Close()
			}

			// Wait till I'm the leader again
			electionHandler.WakeMeUpWhenSeptemberEnds()
			log.Infof("VOLVI A SER LIDER DIOSITO ID %s", hc.id)

			// Start tickers and reset retriesCounter
			retriesCounter = 0
			intervalTicker.Reset(hc.config.Interval)
		}

		if retriesCounter >= hc.config.MaxRetries {
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

			err = setConnection(socket, hc.config.MaxConnectionRetries, hc.config.ConnectionRetryDelay)
			if err != nil {
				continue
			}

			retriesCounter = 0
			intervalTicker.Reset(hc.config.Interval)
			continue
		}

		select {
		case <-intervalTicker.C:
			// Try to connect
			if !socket.IsConnectionOpen() {
				err := socket.OpenConnection()
				if err != nil {
					retryTicker = time.NewTicker(hc.config.RetryDelay)
					continue
				}
			}

			// Send a new heartbeat
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
			log.Debugf("[health checker: %s][service: %s] got heartbeat response '%s'", healthCheckerServiceName+hc.id, serviceName, string(response))
			retriesCounter = 0

		case <-retryTicker.C:
			log.Debugf("[health checker: %s][service: %s] Some error occurs, trying again...", healthCheckerServiceName+hc.id, serviceName)
			retriesCounter += 1

			if !socket.IsConnectionOpen() {
				err := socket.OpenConnection()
				if err != nil {
					continue
				}
			}

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

			log.Info("All goes well in retrying")
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

	log.Infof("Container %s stopped successfully", containerName)
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

	log.Infof("Container %s bringed back to life succesffully!", containerName)
	return nil
}

// setConnection tries to open a connection with the given socket. If an error occurs it tries at most maxRetries times
// with a retryDelay between attempts
func setConnection(socket common.Client, maxRetries int, retryDelay time.Duration) error {
	connectionRetriesCounter := 0
	for {
		if connectionRetriesCounter == maxRetries {
			return fmt.Errorf("%w", errSettingConnection)
		}

		err := socket.OpenConnection()
		if err == nil {
			break
		}

		connectionRetriesCounter += 1
		time.Sleep(retryDelay)
	}
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
