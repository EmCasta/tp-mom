package exchange_middleware

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	name       string
	keys       []string
}

func NewExchangeMiddleware(conn *amqp.Connection, ch *amqp.Channel, name string, keys []string) *ExchangeMiddleware {
	return &ExchangeMiddleware{connection: conn, channel: ch, name: name, keys: keys}
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	q, err := em.channel.QueueDeclare(
		"",    // name
		false, // durability
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}
	for _, k := range em.keys {
		err = em.channel.QueueBind(
			q.Name,  // queue name
			k,       // routing key
			em.name, // exchange
			false,
			nil,
		)
		if err != nil {
			return middleware.ErrMessageMiddlewareMessage
		}
	}

	msgs, err := em.channel.Consume(
		q.Name,     // queue
		"consumer", // consumer
		false,      // auto ack
		false,      // exclusive
		false,      // no local
		false,      // no wait
		nil,        // args
	)
	if err != nil {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	for m := range msgs {
		body := string(m.Body)
		message := middleware.Message{Body: body}
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

func (em *ExchangeMiddleware) StopConsuming() error {
	err := em.channel.Cancel("consumer", false)
	if err != nil {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (em *ExchangeMiddleware) Send(msg middleware.Message) error {
	for _, k := range em.keys {
		if err := em.channel.Publish(
			em.name,
			k,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			},
		); err != nil {
			return middleware.ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	if err := em.channel.Close(); err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	if err := em.connection.Close(); err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	return nil
}
