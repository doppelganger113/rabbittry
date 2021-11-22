package rabbittry

const deadLetterHeader = "x-dead-letter-exchange"

type ExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]interface{}
}

type ExchangeConfigFn func(c *ExchangeConfig)

func NewExchangeConfig(options ...ExchangeConfigFn) ExchangeConfig {
	config := ExchangeConfig{}

	for _, option := range options {
		option(&config)
	}

	return config
}

// WithExchangeName - Durable, non-auto-deleted AMQP exchange name
func WithExchangeName(name string) ExchangeConfigFn {
	return func(c *ExchangeConfig) {
		c.Name = name
	}
}

// WithExchangeType - Exchange type - direct|fanout|topic|x-custom
func WithExchangeType(exchangeType string) ExchangeConfigFn {
	return func(c *ExchangeConfig) {
		c.Type = exchangeType
	}
}

func WithExchangeDurable() ExchangeConfigFn {
	return func(c *ExchangeConfig) {
		c.Durable = true
	}
}

func WithExchangeAutoDelete() ExchangeConfigFn {
	return func(c *ExchangeConfig) {
		c.AutoDelete = true
	}
}

func WithExchangeNoWait() ExchangeConfigFn {
	return func(c *ExchangeConfig) {
		c.NoWait = true
	}
}

func WithExchangeArg(key string, value interface{}) ExchangeConfigFn {
	return func(c *ExchangeConfig) {
		if c.Args == nil {
			c.Args = make(map[string]interface{})
		}
		if _, ok := c.Args[key]; !ok {
			c.Args[key] = value
		}
	}
}

func WithExchangeDeadLetter(queueName string) ExchangeConfigFn {
	return WithExchangeArg(deadLetterHeader, queueName)
}
