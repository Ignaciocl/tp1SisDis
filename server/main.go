package main

import (
	"encoding/json"
	"fmt"
	common "github.com/Ignaciocl/tp1SisdisCommons"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("server", "address")
	v.BindEnv("loop", "period")
	v.BindEnv("loop", "lapse")
	v.BindEnv("log", "level")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	// Parse time.Duration variables and return an error if those variables cannot be parsed
	if _, err := time.ParseDuration(v.GetString("loop.lapse")); err != nil {
		return nil, errors.Wrapf(err, "Could not parse CLI_LOOP_LAPSE env var as time.Duration.")
	}

	if _, err := time.ParseDuration(v.GetString("loop.period")); err != nil {
		return nil, errors.Wrapf(err, "Could not parse CLI_LOOP_PERIOD env var as time.Duration.")
	}

	return v, nil
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

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper) {
	logrus.Infof("action: config | result: success | client_id: %s | server_address: %s | loop_lapse: %v | loop_period: %v | log_level: %s",
		v.GetString("id"),
		v.GetString("server.address"),
		v.GetDuration("loop.lapse"),
		v.GetDuration("loop.period"),
		v.GetString("log.level"),
	)
}

type fileData struct {
	EOF  *bool         `json:"eof"`
	File string        `json:"file"`
	Data []interface{} `json:"data"`
}

type dataToSend struct {
	File string        `json:"file,omitempty"`
	Data []interface{} `json:"data,omitempty"`
	City string        `json:"city,omitempty"`
}

type EOFData struct {
	File string `json:"file"`
	City string `json:"city"`
}

type dataQuery struct {
	sem  chan struct{}
	data map[string]map[string]interface{}
}

type AccData struct {
	QueryResult map[string]interface{} `json:"query_result"`
}

func (dq dataQuery) getQueryValue(key string) (map[string]interface{}, bool) {
	log.Infof("starting to check query value")
	d := <-dq.sem
	value, ok := dq.data[key]
	dq.sem <- d
	log.Infof("finishing checking query value")
	return value, ok
}

func (dq dataQuery) writeQueryValue(data map[string]interface{}) {
	d := <-dq.sem
	dq.data["random"] = data
	dq.sem <- d
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(v)
	clientConfig := ClientConfig{
		ServerAddress:  v.GetString("server.address"),
		ID:             v.GetString("id"),
		LoopLapse:      v.GetDuration("loop.lapse"),
		LoopPeriod:     v.GetDuration("loop.period"),
		ClosingMessage: v.GetString("closingMessage"),
		ClosingBatch:   v.GetString("closingBatch"),
	}
	clientConfigAcc := ClientConfig{
		ID:             v.GetString("id"),
		LoopLapse:      v.GetDuration("loop.lapse"),
		LoopPeriod:     v.GetDuration("loop.period"),
		ClosingMessage: v.GetString("closingMessage"),
		ClosingBatch:   v.GetString("closingBatch"),
		ServerAddress:  "3334",
	}
	client := NewClient(clientConfig)
	clientAcc := NewClient(clientConfigAcc)

	queue, _ := common.InitializeRabbitQueue[dataToSend, dataToSend]("distributor", "rabbit")
	eofStarter, _ := common.InitializeRabbitQueue[EOFData, EOFData]("eofStarter", "rabbit")
	accumulatorInfo, _ := common.InitializeRabbitQueue[AccData, AccData]("accConnection", "rabbit")
	cancelChan := make(chan os.Signal, 1)

	defer queue.Close()
	defer client.CloseConnection()
	defer accumulatorInfo.Close()
	defer eofStarter.Close()
	log.Info("waiting for client")
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	dq := dataQuery{
		sem:  sem,
		data: make(map[string]map[string]interface{}, 0),
	}
	go func() {
		result, _ := accumulatorInfo.ReceiveMessage()
		log.Infof("data received from acc is: %v", result)
		dq.writeQueryValue(result.QueryResult)
	}()
	go func() {
		log.Info("waiting for polling")
		clientAcc.GetConnection(":3334")
		log.Info("received connection")
		for {
			log.Infof("waiting for polling of client")
			clientAcc.ReceiveData()
			log.Infof("polling of client receive correctly")
			if data, ok := dq.getQueryValue("random"); !ok {
				clientAcc.AnswerClient([]byte("{}"))
			} else {
				p, _ := json.Marshal(data)
				clientAcc.AnswerClient(p)
				break
			}
		}
	}()
	go func() {
		client.GetConnection(":9000")
		eofAmount := 0
		city := "montreal"
		for {
			bodyBytes, _ := client.ReceiveData()
			var data fileData
			if len(bodyBytes) < 3 {
				continue
			}
			if err := json.Unmarshal(bodyBytes, &data); err != nil {
				common.FailOnError(err, fmt.Sprintf("error while receiving data for file: %v", string(bodyBytes)))
				continue
			}
			if data.EOF != nil && *data.EOF {
				d := EOFData{
					File: data.File,
					City: city,
				}
				eofStarter.SendMessage(d)
				eofAmount += 1
				if eofAmount == 3 {
					city = "toronto"
				} else if eofAmount == 6 {
					city = "washington"
				}
				if eofAmount == 9 {
					break
				}
				// Trigger eof globally
				continue
			}

			err := queue.SendMessage(dataToSend{
				File: data.File,
				Data: data.Data,
				City: city,
			})
			if err != nil {
				log.Errorf("error happened: %v", err)
			}
		}
		client.CloseConnection()
		log.Info("connection closed")
	}()
	<-cancelChan
}
