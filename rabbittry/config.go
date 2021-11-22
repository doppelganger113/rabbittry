package rabbittry

import "fmt"

type Config struct {
	AmqpURI         string
	ExchangeConfig  ExchangeConfig
	QueueConfig     QueueConfig
	ConsumerConfig  ConsumerConfig
	QueueBindConfig QueueBindConfig
}

type ConfigFn func(c *Config)

func NewConfig(amqpUri, queueName, consumerTag string, options ...ConfigFn) Config {
	c := Config{
		AmqpURI:         amqpUri,
		ExchangeConfig:  ExchangeConfig{},
		QueueConfig:     QueueConfig{Name: queueName},
		ConsumerConfig:  ConsumerConfig{Tag: consumerTag},
		QueueBindConfig: QueueBindConfig{},
	}
	fmt.Printf("Init %+v\n", c)
	for _, optionFn := range options {
		optionFn(&c)
	}

	fmt.Println("Using queue: " + queueName)
	fmt.Printf("Created config %+v\n", c)

	return c
}

func WithExchange(options ...ExchangeConfigFn) ConfigFn {
	return func(c *Config) {
		for _, option := range options {
			option(&c.ExchangeConfig)
		}
	}
}

func WithQueue(options ...QueueConfigFn) ConfigFn {
	return func(c *Config) {
		for _, option := range options {
			option(&c.QueueConfig)
		}
	}
}

func WithConsumer(options ...ConsumerConfigFn) ConfigFn {
	return func(c *Config) {
		for _, option := range options {
			option(&c.ConsumerConfig)
		}
	}
}

func WithQueueBind(options ...QueueBindConfigFn) ConfigFn {
	return func(c *Config) {
		for _, option := range options {
			option(&c.QueueBindConfig)
		}
	}
}
