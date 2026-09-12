package queue_middleware

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
}

func NewQueueMiddleware(conn *amqp.Connection, ch *amqp.Channel, q amqp.Queue) *QueueMiddleware {
	return &QueueMiddleware{connection: conn, channel: ch, queue: q}
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	return nil
}

func (qm *QueueMiddleware) Send(msg middleware.Message) error {
	return nil
}

func (qm *QueueMiddleware) Close() error {
	return nil
}
