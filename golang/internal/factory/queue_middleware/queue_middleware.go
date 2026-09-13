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
	msgs, err := qm.channel.Consume(
		qm.queue.Name, // queue
		"consumer",    // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		return middleware.ErrMessageMiddlewareDisconnected // revisar !!!
	}
	for m := range msgs {
		body := string(m.Body)
		message := middleware.Message{Body: body}
		// TODO: errores??
		ack := func() {
			m.Ack(false)
		}
		nack := func() {
			m.Nack(false, false)
		}
		callbackFunc(message, ack, nack)
	}
	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	err := qm.channel.Cancel("consumer", false)
	if err != nil {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (qm *QueueMiddleware) Send(msg middleware.Message) error {
	// TODO: error interno??
	err := qm.channel.Publish(
		"",
		qm.queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)
	if err != nil {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	if err := qm.channel.Close(); err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	if err := qm.connection.Close(); err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	return nil
}
