package factory

import (
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/exchange_middleware"
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/queue_middleware"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	// TODO: revisar url
	url := fmt.Sprintf("amqp://guest:guest@%s:%d", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil, // TODO: revisar esto!
	)
	if err != nil {
		return nil, err
	}
	return queue_middleware.NewQueueMiddleware(conn, ch, q), nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	// TODO: revisar url
	url := fmt.Sprintf("amqp://guest:guest@%s:%d", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	err = ch.ExchangeDeclare(
		exchange,
		"direct", // TODO: revisar parametros aca
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return exchange_middleware.NewExchangeMiddleware(conn, ch, keys), nil
}
