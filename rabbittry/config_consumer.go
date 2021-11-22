package rabbittry

type ConsumerConfig struct {
	QueueName string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Tag       string
	Args      map[string]interface{}
}

type ConsumerConfigFn func(c *ConsumerConfig)

func NewConsumerConfig(options ...ConsumerConfigFn) ConsumerConfig {
	config := ConsumerConfig{}

	for _, option := range options {
		option(&config)
	}

	return config
}

func WithConsumerAutoAck() ConsumerConfigFn {
	return func(c *ConsumerConfig) {
		c.AutoAck = true
	}
}

func WithConsumerNoLocal() ConsumerConfigFn {
	return func(c *ConsumerConfig) {
		c.NoLocal = true
	}
}

func WithConsumerNoWait() ConsumerConfigFn {
	return func(c *ConsumerConfig) {
		c.NoWait = true
	}
}

func WithConsumerQueue(queue string) ConsumerConfigFn {
	return func(c *ConsumerConfig) {
		c.QueueName = queue
	}
}

func WithConsumerTag(tag string) ConsumerConfigFn {
	return func(c *ConsumerConfig) {
		c.Tag = tag
	}
}

func WithConsumerArg(key string, value interface{}) ConsumerConfigFn {
	return func(c *ConsumerConfig) {
		if c.Args == nil {
			c.Args = make(map[string]interface{})
		}
		if _, ok := c.Args[key]; !ok {
			c.Args[key] = value
		}
	}
}
