package rabbittry

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func NewConsumer(amqpURI, queueName, consumerTag string, options ...ConfigFn) (*Consumer, error) {
	config := NewConfig(amqpURI, queueName, consumerTag, options...)
	exchange := config.ExchangeConfig

	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     config.ConsumerConfig.Tag,
		done:    make(chan error),
	}

	var err error

	log.Printf("Connecting to %q ...", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel: %s", err)
	}

	log.Printf("declaring Exchange (%+v)", config.ExchangeConfig)
	if err = c.channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		exchange.Durable,
		exchange.AutoDelete,
		exchange.Internal,
		exchange.NoWait,
		exchange.Args,
	); err != nil {
		return nil, fmt.Errorf("exchange Declare: %s", err)
	}

	log.Printf("declaring Queue %+v", config.QueueConfig)
	queue, err := c.channel.QueueDeclare(
		config.QueueConfig.Name,
		config.QueueConfig.Durable,
		config.QueueConfig.AutoDelete,
		config.QueueConfig.Exclusive,
		config.QueueConfig.NoWait,
		config.QueueConfig.Args,
	)
	if err != nil {
		return nil, fmt.Errorf("queue Declare: %s", err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers)\n",
		queue.Name, queue.Messages, queue.Consumers,
	)
	log.Printf("binding to exchange %+v\n", config.QueueBindConfig)

	if err = c.channel.QueueBind(
		config.QueueBindConfig.QueueName,
		config.QueueBindConfig.RoutingKey,
		config.QueueBindConfig.Exchange,
		config.QueueBindConfig.NoWait,
		config.QueueBindConfig.Args,
	); err != nil {
		return nil, fmt.Errorf("failed binding queue: %s", err)
	}

	log.Printf("Queue bound to Exchange, starting Consume (%+v)\n", config.QueueBindConfig)

	deliveries, err := c.channel.Consume(
		config.ConsumerConfig.QueueName,
		config.ConsumerConfig.Tag,
		config.ConsumerConfig.AutoAck,
		config.ConsumerConfig.Exclusive,
		config.ConsumerConfig.NoLocal,
		config.ConsumerConfig.NoWait,
		config.ConsumerConfig.Args,
	)
	if err != nil {
		return nil, fmt.Errorf("queue Consume: %s", err)
	}

	go handle(deliveries, c.done)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	log.Println("Waiting for messages...")
	for d := range deliveries {
		log.Printf(
			"got %dB delivery: [%v] %q",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)
		if err := d.Ack(false); err != nil {
			log.Println("failed acknowledging message", err)
		}
		log.Println("Acknowledged")
		time.Sleep(3 * time.Second)
	}
	log.Printf("handle: deliveries channel closed")
	done <- nil
}
