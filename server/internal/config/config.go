package config

import (
	"fmt"
	"github.com/Ignaciocl/tp1SisdisCommons/configloader"
	"gopkg.in/yaml.v3"
)

const configFilePath = "./internal/config/config.yaml"

type ServerConfig struct {
	Protocol                string          `yaml:"protocol"`
	InputPort               int             `yaml:"input_port"`
	OutputPort              int             `yaml:"output_port"`
	PacketLimit             int             `yaml:"packet_limit"`
	ConnectionString        string          `yaml:"connection_string"`
	KeepTryingMessage       string          `yaml:"keep_trying_message"`
	FinishProcessingMessage string          `yaml:"finish_processing_message"`
	Sender                  SenderConfig    `yaml:"sender_config"`
	Receiver                ReceiverConfig  `yaml:"receiver_config"`
	Publisher               PublisherConfig `yaml:"publisher_config"`
}

type SenderConfig struct {
	Consumer             string `yaml:"consumer"`
	AmountOfDistributors int    `yaml:"amount_of_distributors"`
}

type ReceiverConfig struct {
	Queue      string `yaml:"queue_name"`
	RoutingKey string `yaml:"routing_key"`
	Topic      string `yaml:"topic"`
}

type PublisherConfig struct {
	Exchange   string `yaml:"exchange_name"`
	RoutingKey string `yaml:"routing_key"`
	Topic      string `yaml:"topic"`
}

func LoadConfig() (*ServerConfig, error) {
	configFile, err := configloader.GetConfigFileAsBytes(configFilePath)
	if err != nil {
		return nil, err
	}

	var serverConfig ServerConfig
	err = yaml.Unmarshal(configFile, &serverConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing server config file: %s", err)
	}

	return &serverConfig, nil
}
