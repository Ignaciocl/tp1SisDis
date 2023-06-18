package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"time"
)

const configFilepath = ".config/config.yaml"

type HealthCheckerConfig struct {
	Port             int           `yaml:"healthcheck_port"`
	Protocol         string        `yaml:"protocol"`
	PacketLimit      int           `yaml:"packet_limit"`
	Message          string        `yaml:"healthcheck_message"`
	ExpectedResponse string        `yaml:"healthcheck_response"`
	MaxRetries       int           `yaml:"max_retries"`
	RetryDelay       time.Duration `yaml:"retry_delay"`
	Interval         time.Duration `yaml:"interval"`
	Services         []string      `yaml:"services"`
}

func LoadConfig() (*HealthCheckerConfig, error) {
	configFile, err := getConfigFile(configFilepath)
	if err != nil {
		return nil, err
	}

	var healthCheckerConfig HealthCheckerConfig
	err = yaml.Unmarshal(configFile, &healthCheckerConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing health checker config file: %s", err)
	}

	return &healthCheckerConfig, nil
}

func getConfigFile(filepath string) ([]byte, error) {
	configFile, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening config file: %s", err)
	}

	configFileBytes, err := io.ReadAll(configFile)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %s", err)
	}

	return configFileBytes, nil
}
