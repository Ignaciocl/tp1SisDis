package main

import (
	"bytes"
	"net"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopLapse      time.Duration
	LoopPeriod     time.Duration
	ClosingMessage string
	ClosingBatch   string
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Fatalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

func (c *Client) OpenConnection() error {
	for {
		if err := c.createClientSocket(); err != nil {
			log.Errorf("error while openning connection, %v", err)
			return err
		}
		msgReceived := make([]byte, 4)
		if receivedBytes, err := c.conn.Read(msgReceived); err == nil {
			msg := string(msgReceived[0:receivedBytes])
			if receivedBytes == 3 && string(msg) == "ack" {
				log.Infof("connection successful")
				break
			}
			if msg == "nack" {
				log.Info("nack received, waiting a second to ask again")
			} else {
				log.Infof("message: '%s' received, did not understand, waiting a second to try again", msg)
			}
		} else {
			log.Errorf("error while waiting for receive new connection, closing previous and waiting for new: %v", err)
		}
		_ = c.conn.Close()
		c.conn = nil
		time.Sleep(1)
	}
	return nil
}

func (c *Client) GetConnection() {
	listen, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	listen.Accept()
}
func (c *Client) CloseConnection() {
	c.conn.Close()
}

func (c *Client) SendData(bytes []byte, lastBatch bool) error {
	closingMessage := c.config.ClosingMessage
	if lastBatch {
		closingMessage = c.config.ClosingBatch
	}
	bytesToSend := append(bytes, closingMessage...)
	eightKB := 8 * 1024
	size := len(bytesToSend)
	for i := 0; i <= len(bytesToSend); i += eightKB {
		var sending []byte
		if size < i+eightKB {
			sending = bytesToSend[i:size]
		} else {
			sending = bytesToSend[i : i+eightKB]
		}
		amountSent, err := c.conn.Write(sending)
		if err != nil {
			log.Errorf("weird error happened, stopping but something should be checked: %v", err)
			return err
		}
		if dif := len(sending) - amountSent; dif > 0 { // Avoiding short write
			i -= dif
		}
	}
	return nil
}

func (c *Client) ReceiveData() ([]byte, error) {
	eightKB := 8 * 1024
	received := make([]byte, eightKB)
	checkedValue := []byte(c.config.ClosingMessage)
	total := make([]byte, 0)
	for {
		if i, err := c.conn.Read(received); err != nil {
			log.Errorf("error while receiving message, ending receiver: %v", err)
			return nil, err
		} else {
			total = append(total, received[0:i]...)
		}
		if bytes.HasSuffix(total, checkedValue) {
			break
		}
	}
	finalData := total[0 : len(total)-len(c.config.ClosingMessage)]
	return finalData, nil
}
