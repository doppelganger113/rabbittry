package rabbittry

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

type QueueConfigFn func(c *QueueConfig)

func NewQueueConfig(options ...QueueConfigFn) QueueConfig {
	c := QueueConfig{}

	for _, option := range options {
		option(&c)
	}

	return c
}

func WithQueueDurable() QueueConfigFn {
	return func(c *QueueConfig) {
		c.Durable = true
	}
}

func WithQueueAutoDelete() QueueConfigFn {
	return func(c *QueueConfig) {
		c.AutoDelete = true
	}
}

func WithQueueExclusive(isExclusive bool) QueueConfigFn {
	return func(c *QueueConfig) {
		c.Exclusive = isExclusive
	}
}

func WithQueueNoWait(noWait bool) QueueConfigFn {
	return func(c *QueueConfig) {
		c.NoWait = noWait
	}
}

func WithQueueArgument(key string, value interface{}) QueueConfigFn {
	return func(c *QueueConfig) {
		if c.Args == nil {
			c.Args = make(map[string]interface{})
		}
		if _, ok := c.Args[key]; !ok {
			c.Args[key] = value
		}
	}
}

func WithQueueDeadLetter(queueName string) QueueConfigFn {
	return WithQueueArgument(deadLetterHeader, queueName)
}
