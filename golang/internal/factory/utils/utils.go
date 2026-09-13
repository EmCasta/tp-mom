package utils

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	CONNECTION_URL string = "amqp://guest:guest@%s:%d"
)

type callbackFunc func(msg m.Message, ack func(), nack func())

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

func HandleConnectionError(err error, connection *amqp.Connection, channel *amqp.Channel) error {
	if connection.IsClosed() || channel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

func HandleIncomingMessage(msg amqp.Delivery, callback callbackFunc) {
	body := string(msg.Body)
	message := m.Message{Body: body}
	ack := func() {
		msg.Ack(false)
	}
	nack := func() {
		msg.Nack(false, false)
	}
	callback(message, ack, nack)
}

func CreateMessage(body string) amqp.Publishing {
	return amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	}
}
