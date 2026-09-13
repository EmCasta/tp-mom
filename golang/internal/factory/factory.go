package factory

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/exchange_middleware"
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/queue_middleware"
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/utils"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, ch, err := utils.Connect(connectionSettings)
	if err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return queue_middleware.NewQueueMiddleware(conn, ch, q), nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, ch, err := utils.Connect(connectionSettings)
	if err != nil {
		return nil, err
	}
	err = ch.ExchangeDeclare(
		exchange,
		amqp.ExchangeDirect,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return exchange_middleware.NewExchangeMiddleware(conn, ch, exchange, keys), nil
}
