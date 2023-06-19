package config

import (
	"fmt"
	"github.com/Ignaciocl/tp1SisdisCommons/configloader"
	"gopkg.in/yaml.v3"
	"time"
)

const configFilepath = "config/config.yaml"

type HealthCheckerConfig struct {
	Port                 int           `yaml:"healthcheck_port"`
	Protocol             string        `yaml:"protocol"`
	PacketLimit          int           `yaml:"packet_limit"`
	Message              string        `yaml:"healthcheck_message"`
	ExpectedResponse     string        `yaml:"healthcheck_response"`
	MaxRetries           int           `yaml:"max_retries"`
	MaxConnectionRetries int           `yaml:"max_connection_retries"`
	RetryDelay           time.Duration `yaml:"retry_delay"`
	ConnectionRetryDelay time.Duration `yaml:"connection_retry_delay"`
	Interval             time.Duration `yaml:"interval"`
	GraceTime            time.Duration `yaml:"grace_time"`
	Services             []string      `yaml:"services"`
	TTL                  int           `yaml:"ttl"`
}

func LoadConfig() (*HealthCheckerConfig, error) {
	configFile, err := configloader.GetConfigFileAsBytes(configFilepath)
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
