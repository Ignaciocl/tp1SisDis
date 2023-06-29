package config

import (
	"fmt"
	"github.com/Ignaciocl/tp1SisdisCommons/configloader"
	"gopkg.in/yaml.v3"
)

const configFilepath = "./internal/config/config.yaml"

type DistributorConfig struct {
	ConnectionString   string            `yaml:"connection_string"`
	WorkerQueues       map[string]string `yaml:"worker_queues"`
	MaxAmountReceivers int               `yaml:"max_amount_receivers"`
	NecessaryAmount    int               `yaml:"necessary_amount"`
}

func LoadConfig() (*DistributorConfig, error) {
	configFile, err := configloader.GetConfigFileAsBytes(configFilepath)
	if err != nil {
		return nil, err
	}

	var distributorConfig DistributorConfig
	err = yaml.Unmarshal(configFile, &distributorConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing distributor config file: %s", err)
	}

	return &distributorConfig, nil
}
