package rabbittry

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type ConsumerSession struct {
	conn                  *amqp.Connection
	channel               *amqp.Channel
	tag                   string
	done                  chan error
	isReady               bool
	notifyConnectionClose chan *amqp.Error
	notifyChannelClose    chan *amqp.Error
	notifyConfirm         chan amqp.Confirmation
	config                Config
}

func NewConsumerSession(amqpURI, queueName, consumerTag string, options ...ConfigFn) (*ConsumerSession, error) {
	config := NewConfig(amqpURI, queueName, consumerTag, options...)

	session := &ConsumerSession{
		conn:    nil,
		channel: nil,
		tag:     config.ConsumerConfig.Tag,
		done:    make(chan error),
		config:  config,
	}

	go session.handleReconnection(amqpURI)

	return session, nil
}

func (session *ConsumerSession) connect(amqpUri string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(amqpUri)
	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	fmt.Println("connected")

	return conn, nil
}

// init will initialize channel & declare queue
func (session *ConsumerSession) init(conn *amqp.Connection) error {
	exchange := session.config.ExchangeConfig
	queueConfig := session.config.QueueConfig

	fmt.Println("getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %s", err)
	}

	err = channel.Confirm(false)
	if err != nil {
		return fmt.Errorf("failed confirming channel: %v", err)
	}

	fmt.Printf("declaring Exchange (%+v)\n", session.config.ExchangeConfig)
	if err = channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		exchange.Durable,
		exchange.AutoDelete,
		exchange.Internal,
		exchange.NoWait,
		exchange.Args,
	); err != nil {
		return fmt.Errorf("exchange Declare: %s", err)
	}

	fmt.Printf("declaring Queue %+v\n", session.config.QueueConfig)
	queue, err := channel.QueueDeclare(
		queueConfig.Name,
		queueConfig.Durable,
		queueConfig.AutoDelete,
		queueConfig.Exclusive,
		queueConfig.NoWait,
		queueConfig.Args,
	)
	if err != nil {
		return fmt.Errorf("queue Declare: %s", err)
	}

	fmt.Printf("declared Queue (%q %d messages, %d consumers)\n",
		queue.Name, queue.Messages, queue.Consumers,
	)

	fmt.Printf("binding to exchange %+v\n", session.config.QueueBindConfig)

	if err = channel.QueueBind(
		session.config.QueueBindConfig.QueueName,
		session.config.QueueBindConfig.RoutingKey,
		session.config.QueueBindConfig.Exchange,
		session.config.QueueBindConfig.NoWait,
		session.config.QueueBindConfig.Args,
	); err != nil {
		return fmt.Errorf("failed binding queue: %s", err)
	}

	fmt.Printf("Queue bound to Exchange, starting Consume (%+v)\n", session.config.QueueBindConfig)

	session.changeChannel(channel)
	session.isReady = true

	deliveries, err := session.channel.Consume(
		session.config.ConsumerConfig.QueueName,
		session.config.ConsumerConfig.Tag,
		session.config.ConsumerConfig.AutoAck,
		session.config.ConsumerConfig.Exclusive,
		session.config.ConsumerConfig.NoLocal,
		session.config.ConsumerConfig.NoWait,
		session.config.ConsumerConfig.Args,
	)
	if err != nil {
		return fmt.Errorf("queue Consume: %s", err)
	}

	go handleSession(deliveries, session.done)

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *ConsumerSession) changeConnection(connection *amqp.Connection) {
	session.conn = connection
	session.notifyConnectionClose = make(chan *amqp.Error)
	session.conn.NotifyClose(session.notifyConnectionClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *ConsumerSession) changeChannel(channel *amqp.Channel) {
	session.channel = channel
	session.notifyChannelClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	session.channel.NotifyClose(session.notifyChannelClose)
	session.channel.NotifyPublish(session.notifyConfirm)
}

func (session *ConsumerSession) handleReconnection(amqpUri string) {
	for {
		session.isReady = false
		fmt.Println("Attempting to connect...")

		conn, err := session.connect(amqpUri)
		if err != nil {
			fmt.Println("failed to connect, retrying...", err)

			select {
			case <-session.done:
				fmt.Println("Session is done, ending handleReconnection")
				return
			case <-time.After(4 * time.Second):
			}
			continue
		}

		fmt.Println("Successfully connected")

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

func (session *ConsumerSession) handleReInit(conn *amqp.Connection) (connected bool) {
	for {
		session.isReady = false

		fmt.Println("handling reInit")
		err := session.init(conn)
		if err != nil {
			fmt.Println("failed to initialize channel, retrying...", err)

			select {
			case <-session.done:
				return true
			case <-time.After(3 * time.Second):
			}
			continue
		}

		fmt.Println("Session init success")

		select {
		case <-session.done:
			return true
		case <-session.notifyConnectionClose:
			fmt.Println("connection closed, retrying...")
			return false
		case <-session.notifyChannelClose:
			fmt.Println("channel closed, re-running init")
		}
	}
}

func (session *ConsumerSession) Shutdown() error {
	if !session.isReady {
		return errors.New("already closed")
	}

	// will close() the deliveries channel
	if session.channel != nil {
		if err := session.channel.Cancel(session.tag, false); err != nil {
			return fmt.Errorf("consumer cancel failed: %s", err)
		}
		fmt.Println("Session closed")
	}

	if session.conn != nil {
		if err := session.conn.Close(); err != nil {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
		fmt.Println("Connection closed")
	}

	close(session.done)
	session.isReady = false
	defer fmt.Println("AMQP shutdown OK")

	// wait for handle() to exit
	return <-session.done
}

func handleSession(deliveries <-chan amqp.Delivery, done <-chan error) {
	for {
		select {
		case <-done:
			fmt.Println("Stopping session")
			return
		case d, ok := <-deliveries:
			if !ok {
				break
			}
			fmt.Println("Waiting for messages...")
			fmt.Printf(
				"got %dB delivery: [%v] %q\n",
				len(d.Body),
				d.DeliveryTag,
				d.Body,
			)
			if err := d.Ack(false); err != nil {
				fmt.Println("failed acknowledging message", err)
			}
			fmt.Println("Acknowledged")
			time.Sleep(5 * time.Second)
		}
	}

	fmt.Println("handle: deliveries channel closed")
}
