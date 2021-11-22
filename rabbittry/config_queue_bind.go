package rabbittry

type QueueBindConfig struct {
	QueueName  string
	RoutingKey string
	Exchange   string
	NoWait     bool
	Args       map[string]interface{}
}

type QueueBindConfigFn func(c *QueueBindConfig)

func NewQueueBindConfig(options ...QueueBindConfigFn) QueueBindConfig {
	c := QueueBindConfig{}

	for _, option := range options {
		option(&c)
	}

	return c
}

func WithQueueBindName(name string) QueueBindConfigFn {
	return func(c *QueueBindConfig) {
		c.QueueName = name
	}
}

func WithQueueBindKey(key string) QueueBindConfigFn {
	return func(c *QueueBindConfig) {
		c.RoutingKey = key
	}
}

func WithQueueBindExchange(exchange string) QueueBindConfigFn {
	return func(c *QueueBindConfig) {
		c.Exchange = exchange
	}
}

func WithQueueBindArg(key string, value interface{}) QueueBindConfigFn {
	return func(c *QueueBindConfig) {
		if c.Args == nil {
			c.Args = make(map[string]interface{})
		}
		if _, ok := c.Args[key]; !ok {
			c.Args[key] = value
		}
	}
}

func WithQueueBindDeadLetter(queueName string) QueueBindConfigFn {
	return WithQueueBindArg(deadLetterHeader, queueName)
}
