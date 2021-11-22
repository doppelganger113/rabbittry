package main

import (
	"flag"
	"fmt"
	"github.com/doppelganger29/rabbittry/rabbittry"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("exchange", "my-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("exchange-type", "fanout", "Exchange type - direct|fanout|topic|x-custom")
	queue        = flag.String("queue", "job_queue", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("key", "", "AMQP binding key")
	consumerTag  = flag.String("consumer-tag", "simple-consumer", "AMQP consumer tag (should not be blank)")
	lifetime     = flag.Duration("lifetime", 5*time.Second, "lifetime of process before shutdown (0s=infinite)")
)

type MyProcessor struct{}

func (p *MyProcessor) Init(channel *amqp.Channel) error {
	return nil
}

func (p *MyProcessor) Process(data []byte) error {
	return nil
}

func main() {
	consumerConfig := rabbittry.ConsumerConfig{
		QueueName: "",
		AutoAck:   false,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Tag:       "",
		Args:      nil,
	}
	myProcessor := &MyProcessor{}

	errChannel := make(chan error)
	go listenForInterrupt(errChannel)

	session, err := rabbittry.NewConsumerBestSession(
		*uri, *consumerTag, consumerConfig, myProcessor, rabbittry.ConsumerOptions{},
	)
	if err != nil {
		log.Fatalln("failed creating session", err)
	}

	fatalErr := <-errChannel
	log.Println("shutting down", fatalErr)
	if err = session.Shutdown(); err != nil {
		log.Fatalf("error during shutdown: %s", err)
	}
}

func listenForInterrupt(errChannel chan<- error) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	errChannel <- fmt.Errorf("%s", <-c)
}
