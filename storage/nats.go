/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

// Google cloud Pub/Sub implementation of Storage
package storage

import (
	"context"
	"errors"
	stan "github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Nats struct {
	Connection    stan.Conn
	channels      []string
	subscriptions []*stan.Subscription
}

func NatsConnMonitor(nats *Nats, clusterID, clientID string, opts ...stan.Option) {
	t := time.NewTicker(3 * time.Second)
	for range t.C {
		if nats.Connection == nil {
			log.Warnf("reestablish connection to the NATS")
			conn, err := stan.Connect(clusterID, clientID, opts...)
			if err != nil {
				log.Error(err)
			}
			log.Info("connection to the NATS established")
			nats.Connection = conn
		}
	}
}

func NewNats(clusterID, clientID string, opts ...stan.Option) (*Nats, error) {
	conn, err := stan.Connect(clusterID, clientID, opts...)
	if err != nil {
		return nil, err
	}

	n := &Nats{
		Connection: conn,
	}

	go NatsConnMonitor(n, clusterID, clientID, opts...)

	return n, nil
}

func (n *Nats) InitChannelsStorage(channels []string) error {
	n.channels = channels
	return nil
}

// Put stores message to topic.
func (n *Nats) Put(topic string, msg []byte) error {
	return n.Connection.Publish(topic, msg) // sync call, wait for ACK from NATS Streaming
}

// Get reads one message from the topic and closes channel.
func (n *Nats) Get(ctx context.Context, topic string) ([]byte, error) {
	var data []byte
	sub, err := SubscriptionMgr(ctx, n.Connection, topic, func(m *stan.Msg) {
		data = m.Data
		if err := m.Ack(); err != nil {
			log.Errorf("failed to ack message, %v", err)
		}
	}, stan.SetManualAckMode())
	n.subscriptions = append(n.subscriptions, sub)
	return data, err
}

// GetStream reads a stream of messages from topic and writes them to the channel.
func (n *Nats) GetStream(ctx context.Context, topic string) (<-chan []byte, <-chan error) {
	ch, errch := make(chan []byte), make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		sub, err := SubscriptionMgr(ctx, n.Connection, topic, func(m *stan.Msg) {
			ch <- m.Data
			if err := m.Ack(); err != nil {
				log.Errorf("failed to ack message, %v", err)
			}
		}, stan.SetManualAckMode())
		n.subscriptions = append(n.subscriptions, sub)
		if err != nil {
			errch <- err
		}
	}()
	wg.Wait()
	return ch, errch
}

// Detele does not work for Nats.
func (n *Nats) Delete(key string) error {
	return errors.New("Not implemented in Nats")
}

// Close stops all running goroutines related to topics.
func (n *Nats) Close() error {
	for _, sub := range n.subscriptions {
		if err := (*sub).Unsubscribe(); err != nil {
			return err
		}
		if err := (*sub).Close(); err != nil {
			return err
		}
	}
	return n.Connection.Close()
}

func SubscriptionMgr(ctx context.Context, conn stan.Conn, subject string, cb stan.MsgHandler, opts ...stan.SubscriptionOption) (*stan.Subscription, error) {
	sub, err := conn.QueueSubscribe(subject, subject, cb, opts...)
	if err != nil {
		return nil, err
	}
	subptr := &sub
	var validSub bool
	t := time.NewTicker(3 * time.Second)
	go func() {
	OuterLoop:
		for {
			select {
			case <-t.C:
				if sub == nil || (sub != nil && !sub.IsValid()) {
					if conn.NatsConn() != nil {
						validSub = false
						log.Errorf("Subscription to %s is not valid, recreate subcription", subject)
						sub, err = conn.QueueSubscribe(subject, subject, cb, opts...)
						if err != nil {
							panic(err)
						}
						subptr = &sub
					}
				} else {
					if !validSub {
						log.Infof("Subscription to %s restored", subject)
					}
					validSub = true
				}
			case <-ctx.Done():
				log.Warnf("stop resubscriber for %s", subject)
				subptr = nil
				break OuterLoop
			}
		}
	}()
	return subptr, nil
}
