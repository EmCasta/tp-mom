package exchange_middleware

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/utils"
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	EXCHANGE_CONSUMER_TAG string = "exchange-consumer"
)

type ExchangeMiddleware struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	name       string
	keys       []string
	consumer   string
}

func NewExchangeMiddleware(conn *amqp.Connection, ch *amqp.Channel, name string, keys []string) *ExchangeMiddleware {
	return &ExchangeMiddleware{connection: conn, channel: ch, name: name, keys: keys, consumer: EXCHANGE_CONSUMER_TAG}
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	q, err := em.channel.QueueDeclare(
		"",    // name
		true,  // durability
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return utils.HandleConnectionError(err, em.connection, em.channel)
	}
	for _, k := range em.keys {
		err = em.channel.QueueBind(
			q.Name,  // queue name
			k,       // routing key
			em.name, // exchange
			false,   // no-wait
			nil,
		)
		if err != nil {
			return utils.HandleConnectionError(err, em.connection, em.channel)
		}
	}
	msgs, err := em.channel.Consume(
		q.Name,      // queue
		em.consumer, // consumer
		false,       // auto ack
		false,       // exclusive
		false,       // no local
		false,       // no wait
		nil,         // args
	)
	if err != nil {
		return utils.HandleConnectionError(err, em.connection, em.channel)
	}
	for m := range msgs {
		utils.HandleIncomingMessage(m, callbackFunc)
	}
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	return utils.CancelConsumption(em.channel, em.consumer)
}

func (em *ExchangeMiddleware) Send(msg middleware.Message) error {
	for _, k := range em.keys {
		if err := em.channel.Publish(
			em.name,
			k,
			false,
			false,
			utils.CreateMessage(msg.Body),
		); err != nil {
			return utils.HandleConnectionError(err, em.connection, em.channel)
		}
	}
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return utils.CloseConnection(em.connection, em.channel)
}
