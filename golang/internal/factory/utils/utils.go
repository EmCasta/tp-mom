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

// Establece una conexión amqp con el middleware y un canal de comunicación para procesamiento
// de los mensajes. Retorna error en caso de no poder completar con éxito el establecimiento
// de conexión
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

// Cierra el canal de comunicación y la conexión con el middleware. Retorna
// ErrMessageMiddlewareClose en caso de error
func CloseConnection(connection *amqp.Connection, channel *amqp.Channel) error {
	if err := channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	if err := connection.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

// Provoca que el consumidor deje de recibir mensajes de forma limpia. Retorna
// ErrMessageMiddlewareDisconnected en caso de error
func CancelConsumption(channel *amqp.Channel, consumer string) error {
	err := channel.Cancel(consumer, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

// Retorna ErrMessageMiddlewareDisconnected en caso de desconexión con el middleware,
// ErrMessageMiddlewareMessage en caso de error interno
func HandleConnectionError(err error, connection *amqp.Connection, channel *amqp.Channel) error {
	if connection.IsClosed() || channel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

// Función a ser llamada ante la llegada de mensajes
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

// Configura el tipo de mensaje a enviar/recibir
func CreateMessage(body string) amqp.Publishing {
	return amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	}
}
