/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

// Google cloud Pub/Sub implementation of Storage
package storage

import (
	"cloud.google.com/go/pubsub"
	"context"
	"google.golang.org/api/option"
	"sync"
	"time"
)

type PubSub struct {
	ctx           context.Context
	client        *pubsub.Client
	topics        map[string]*pubsub.Topic        // name of topic => *pubsub.Topic mapping
	subscriptions map[string]*pubsub.Subscription // name of subscription => *pubsub.Subscription mapping
}

func NewPubSub(project string, opts ...option.ClientOption) (*PubSub, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project, opts...)
	if err != nil {
		return nil, err
	}
	return &PubSub{
		client:        client,
		topics:        make(map[string]*pubsub.Topic),
		subscriptions: make(map[string]*pubsub.Subscription),
	}, nil
}

func (p *PubSub) InitChannelsStorage(channels []string) error {
	p.ctx = context.Background()
	for _, channel := range channels {
		var topic *pubsub.Topic
		topic = p.client.Topic(channel)
		topicExists, err := topic.Exists(p.ctx)
		if err != nil {
			return err
		}
		if !topicExists {
			topic, err = p.client.CreateTopic(p.ctx, channel)
			if err != nil {
				return err
			}
		}

		topic.EnableMessageOrdering = true
		p.topics[channel] = topic

		var sub *pubsub.Subscription
		sub = p.client.Subscription(channel)
		subExists, err := sub.Exists(p.ctx)
		if !subExists {
			sub, err = p.client.CreateSubscription(p.ctx, channel, pubsub.SubscriptionConfig{
				Topic:                 topic,
				AckDeadline:           10 * time.Second,
				ExpirationPolicy:      720 * time.Hour,
				EnableMessageOrdering: true,
			})
			if err != nil {
				return nil
			}
		}

		sub.ReceiveSettings.Synchronous = true
		p.subscriptions[channel] = sub
	}
	return nil
}

// Put stores message to topic.
func (p *PubSub) Put(topic string, msg []byte) error {
	res := p.topics[topic].Publish(p.ctx, &pubsub.Message{
		Data:        msg,
		OrderingKey: "0",
	})
	<-res.Ready()
	return nil
}

// Get reads one message from the topic and closes channel.
func (p *PubSub) Get(_ context.Context, topic string) ([]byte, error) {
	var err error
	ctx, _ := context.WithCancel(context.Background())
	ch, errch := make(chan []byte), make(chan error)
	go func(ch chan []byte, errch chan error) {
		err = p.subscriptions[topic].Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			ch <- m.Data
			m.Ack()
			return
		})
		if err != nil && err == context.Canceled {
			errch <- err
		}
	}(ch, errch)

	select {
	case data := <-ch:
		return data, nil
	case err = <-errch:
		return nil, err
	}
}

// GetStream reads a stream of messages from topic and writes them to the channel.
func (p *PubSub) GetStream(_ context.Context, topic string) (<-chan []byte, <-chan error) {
	var err error
	ch, errch := make(chan []byte), make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		err = p.subscriptions[topic].Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
			ch <- m.Data
			m.Ack()
		})
		if err != nil && err == context.Canceled {
			errch <- err
		}
	}()
	wg.Wait()
	return ch, errch
}

// Detele deletes topic and subscription specified by key.
func (p *PubSub) Delete(key string) error {
	err := p.topics[key].Delete(p.ctx)
	if err != nil {
		return err
	}
	err = p.subscriptions[key].Delete(p.ctx)
	return err
}

// Close stops all running goroutines related to topics.
func (p *PubSub) Close() error {
	for _, topic := range p.topics {
		topic.Stop()
	}
	return nil
}
