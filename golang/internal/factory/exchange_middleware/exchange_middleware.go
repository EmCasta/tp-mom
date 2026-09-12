package exchange_middleware

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	keys       []string
}

func NewExchangeMiddleware(conn *amqp.Connection, ch *amqp.Channel, keys []string) *ExchangeMiddleware {
	return &ExchangeMiddleware{connection: conn, channel: ch, keys: keys}
}

func (qm *ExchangeMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	return nil
}

func (qm *ExchangeMiddleware) StopConsuming() error {
	return nil
}

func (qm *ExchangeMiddleware) Send(msg middleware.Message) error {
	return nil
}

func (qm *ExchangeMiddleware) Close() error {
	return nil
}
