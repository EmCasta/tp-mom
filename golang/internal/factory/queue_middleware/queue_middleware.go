package queue_middleware

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/utils"
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QUEUE_CONSUMER_TAG string = "queue-consumer"
)

type QueueMiddleware struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
	consumer   string
}

func NewQueueMiddleware(conn *amqp.Connection, ch *amqp.Channel, q amqp.Queue) *QueueMiddleware {
	return &QueueMiddleware{connection: conn, channel: ch, queue: q, consumer: QUEUE_CONSUMER_TAG}
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	msgs, err := qm.channel.Consume(
		qm.queue.Name, // queue
		qm.consumer,   // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	// TODO: agregar QoS?
	if err != nil {
		return utils.HandleConnectionError(err, qm.connection, qm.channel)
	}
	// leer mensajes entrantes
	for m := range msgs {
		utils.HandleIncomingMessage(m, callbackFunc)
	}
	// TODO: y caso de error?
	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	return utils.CancelConsumption(qm.channel, qm.consumer)
}

func (qm *QueueMiddleware) Send(msg middleware.Message) error {
	err := qm.channel.Publish(
		"",
		qm.queue.Name,
		false,
		false,
		utils.CreateMessage(msg.Body),
	)
	if err != nil {
		return utils.HandleConnectionError(err, qm.connection, qm.channel)
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	return utils.CloseConnection(qm.connection, qm.channel)
}
