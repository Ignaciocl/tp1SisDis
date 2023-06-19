package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
	"tp1SisDis/healthchecker/config"
	"tp1SisDis/healthchecker/entity"
)

const (
	logLevelEnv     = "LOG_LEVEL"
	defaultLogLevel = "DEBUG"
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
	time.Sleep(5 * time.Second)
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

/*package main

import (
	"context"
	"io"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

func main() {
	ctx := context.Background()
	socketPath := "unix:///Users/litorresetti/.colima/default/docker.sock"
	cli, err := client.NewClientWithOpts(client.WithHost(socketPath), client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	reader, err := cli.ImagePull(ctx, "docker.io/library/alpine", types.ImagePullOptions{})
	if err != nil {
		panic(err)
	}
	io.Copy(os.Stdout, reader)

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: "alpine",
		Cmd:   []string{"echo", "hello world"},
	}, nil, nil, nil, "")
	if err != nil {
		panic(err)
	}

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		panic(err)
	}

	statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			panic(err)
		}
	case <-statusCh:
	}

	out, err := cli.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStdout: true})
	if err != nil {
		panic(err)
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)
}
*/
