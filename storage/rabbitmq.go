package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/newity/crawler/storageadapter"
	"github.com/streadway/amqp"
	"sync"
)

type Rabbit struct {
	Connection *amqp.Connection
	channels   []string
	RabbitCh   *amqp.Channel
	confirms   chan amqp.Confirmation
}

func NewRabbit(rabbitUser, rabbitPass, server string) (*Rabbit, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", rabbitUser, rabbitPass, server))
	if err != nil {
		return nil, err
	}

	return &Rabbit{
		Connection: conn,
	}, nil
}

func (r *Rabbit) InitChannelsStorage(channels []string) error {
	r.channels = channels

	ch, err := r.Connection.Channel()
	if err != nil {
		return fmt.Errorf("Failed to open a RabbitMQ channel")
	}
	r.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err = ch.Confirm(false); err != nil {
		return fmt.Errorf("failed to put RabbitMQ channel into confirm mode")
	}

	r.RabbitCh = ch
	for _, channel := range channels {
		_, err = ch.QueueDeclare(
			channel, // name
			false,   // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		if err != nil {
			return fmt.Errorf("Failed to declare a queue %s", channel)
		}
	}

	return nil
}

// Put stores message to topic.
func (r *Rabbit) Put(topic string, msg []byte) error {
	err := r.RabbitCh.Publish(
		"",    // exchange
		topic, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        msg,
		})
	if err != nil {
		return err
	}
	if confirmed := <-r.confirms; !confirmed.Ack {
		return fmt.Errorf("delivery (tag %d) is not confirmed by RabbitMQ", confirmed.DeliveryTag)
	}
	return nil
}

// Get reads one message from the topic.
func (r *Rabbit) Get(topic string) ([]byte, error) {
	msgs, err := r.RabbitCh.Consume(
		topic, // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to register a consumer for a queue %s", topic)
	}

	d := <-msgs
	return d.Body, nil
}

// GetStream reads a stream of messages from topic and writes them to the channel.
func (r *Rabbit) GetStream(ctx context.Context, topic string) (<-chan []byte, <-chan error) {
	ch, errch := make(chan []byte), make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)

	msgs, err := r.RabbitCh.Consume(
		topic, // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		errch <- fmt.Errorf("Failed to register a consumer for a queue %s", topic)
		return ch, errch
	}

	go func() {
		wg.Done()
		for d := range msgs {
			encoded, err := storageadapter.EncodeRabbitMsg(&d)
			if err != nil {
				errch <- fmt.Errorf("Failed to encode a msg (MessageId %s) from the queue %s, stop listen queue", d.MessageId, topic)
				break
			}
			ch <- encoded
			//if err = d.Ack(false); err != nil {
			//	errch <- fmt.Errorf("Failed to ack a msg (MessageId %s) in the queue %s, stop listen queue", d.MessageId, topic)
			//	break
			//}
		}
	}()
	wg.Wait()
	return ch, errch
}

// Detele does not work for RabbitMQ.
func (r *Rabbit) Delete(key string) error {
	return errors.New("Not implemented in RabbitMQ")
}

// Close stops all running goroutines related to topics.
func (r *Rabbit) Close() error {
	return r.RabbitCh.Close()
}
