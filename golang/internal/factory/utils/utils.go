package utils

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	CONNECTION_URL string = "amqp://guest:guest@%s:%d"
)

func Connect(connectionSettings m.ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf(CONNECTION_URL, connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return conn, ch, err
}

func CloseConnection(connection *amqp.Connection, channel *amqp.Channel) error {
	if err := channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	if err := connection.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

func CancelConsumption(channel *amqp.Channel, consumer string) error {
	err := channel.Cancel(consumer, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}
