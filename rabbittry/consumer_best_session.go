package rabbittry

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type ConsumerBestSession struct {
	amqpUri               string
	tag                   string
	conn                  *amqp.Connection
	channel               *amqp.Channel
	done                  chan error
	isReady               bool
	notifyConnectionClose chan *amqp.Error
	notifyChannelClose    chan *amqp.Error
	notifyConfirm         chan amqp.Confirmation
	consumerConfig        ConsumerConfig
	processor             QueueProcessor
	retryTimeout          time.Duration
	logger                Logger
}

type QueueProcessor interface {
	Init(channel *amqp.Channel) error
	Process(data []byte) error
}

type Logger interface {
	Info(msg string)
	Debug(msg string)
	Warn(msg string)
}

type ConsumerOptions struct {
	logger       Logger
	retryTimeout time.Duration
}

func NewConsumerBestSession(
	amqpURI, sessionTag string, consumerConfig ConsumerConfig, processor QueueProcessor, options ConsumerOptions,
) (*ConsumerBestSession, error) {
	session := &ConsumerBestSession{
		amqpUri:        amqpURI,
		conn:           nil,
		channel:        nil,
		tag:            sessionTag,
		done:           make(chan error),
		logger:         options.logger,
		retryTimeout:   options.retryTimeout,
		processor:      processor,
		consumerConfig: consumerConfig,
	}

	go session.handleReconnection(amqpURI)

	return session, nil
}

func (session *ConsumerBestSession) info(msg string) {
	if session.logger != nil {
		session.logger.Info(msg)
	}
}

func (session *ConsumerBestSession) debug(msg string) {
	if session.logger != nil {
		session.logger.Debug(msg)
	}
}

func (session *ConsumerBestSession) warn(msg string) {
	if session.logger != nil {
		session.logger.Debug(msg)
	}
}

func (session *ConsumerBestSession) connect(amqpUri string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(amqpUri)
	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	session.info("connected")

	return conn, nil
}

// init will initialize channel & declare queue
func (session *ConsumerBestSession) init(conn *amqp.Connection) error {
	session.info("establishing channel")

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed establishing channel: %s", err)
	}

	if err = session.processor.Init(channel); err != nil {
		return err
	}

	session.changeChannel(channel)
	session.isReady = true

	deliveries, err := session.channel.Consume(
		session.consumerConfig.QueueName,
		session.consumerConfig.Tag,
		session.consumerConfig.AutoAck,
		session.consumerConfig.Exclusive,
		session.consumerConfig.NoLocal,
		session.consumerConfig.NoWait,
		session.consumerConfig.Args,
	)
	if err != nil {
		return fmt.Errorf("failed creating consume on channel: %s", err)
	}

	go func() {
		session.consume(deliveries)
	}()

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *ConsumerBestSession) changeConnection(connection *amqp.Connection) {
	session.conn = connection
	session.notifyConnectionClose = make(chan *amqp.Error)
	session.conn.NotifyClose(session.notifyConnectionClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *ConsumerBestSession) changeChannel(channel *amqp.Channel) {
	session.channel = channel
	session.notifyChannelClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	session.channel.NotifyClose(session.notifyChannelClose)
	session.channel.NotifyPublish(session.notifyConfirm)
}

func (session *ConsumerBestSession) handleReconnection(amqpUri string) {
	for {
		session.isReady = false
		session.info("Attempting to connect...")

		conn, err := session.connect(amqpUri)
		if err != nil {
			session.info("failed to connect: " + err.Error() + ", retrying...")

			select {
			case <-session.done:
				session.info("Session done, ending handleReconnection")
				return
			case <-time.After(session.retryTimeout):
			}
			continue
		}
		session.info("Connected")

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

func (session *ConsumerBestSession) handleReInit(conn *amqp.Connection) (connected bool) {
	for {
		session.isReady = false

		session.info("handling reInit")
		err := session.init(conn)
		if err != nil {
			session.info(fmt.Sprintf("failed to initialize channel: %v, retrying...", err))

			select {
			case <-session.done:
				return true
			case <-time.After(session.retryTimeout):
			}
			continue
		}

		session.info("session initialized")

		select {
		case <-session.done:
			return true
		case <-session.notifyConnectionClose:
			session.info("connection closed, retrying...")
			return false
		case <-session.notifyChannelClose:
			session.info("channel closed, re-running init")
		}
	}
}

func (session *ConsumerBestSession) Shutdown() error {
	if !session.isReady {
		return errors.New("already closed")
	}

	// will close() the deliveries channel
	if session.channel != nil {
		if err := session.channel.Cancel(session.tag, false); err != nil {
			return fmt.Errorf("consumer cancel failed: %s", err)
		}
		session.info("Session closed")
	}

	if session.conn != nil {
		if err := session.conn.Close(); err != nil {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
		session.info("Connection closed")
	}

	close(session.done)
	session.isReady = false
	defer session.info("AMQP shutdown OK")

	// wait for handle() to exit
	return <-session.done
}

func (session *ConsumerBestSession) consume(deliveries <-chan amqp.Delivery) {
	session.info("Waiting for messages...")
	for {
		select {
		case <-session.done:
			session.info("Stopping session")
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			session.debug(fmt.Sprintf(
				"got %dB delivery: [%v] %q\n",
				len(d.Body),
				d.DeliveryTag,
				d.Body,
			))

			err := session.processor.Process(d.Body)
			if err != nil {
				session.warn(fmt.Sprintf("failed processing: %v", err))
				if err = d.Nack(true, true); err != nil {
					session.warn(fmt.Sprintf("failed nacking message: %v", err))
				}

			} else {
				if err = d.Ack(false); err != nil {
					session.warn(fmt.Sprintf("failed acking message: %v", err))
				} else {
					session.debug("Acknowledged")
				}
			}

			time.Sleep(5 * time.Second)
		}
	}
}
