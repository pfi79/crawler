/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

// Google cloud Pub/Sub implementation of Storage
package storage

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/avast/retry-go/v3"
	"github.com/go-redis/redis/v7"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"time"
)

type Redis struct {
	Client redis.UniversalClient
}

func NewRedis(password string, addrs []string, withTLS bool, rootCAs []string) (*Redis, error) {
	var (
		redisOpts *redis.UniversalOptions
		client    redis.UniversalClient
	)

	if withTLS {
		certPool := x509.NewCertPool()

		for _, rootCA := range rootCAs {
			cert, err := ioutil.ReadFile(rootCA)
			if err != nil {
				return nil, fmt.Errorf("failed to read root CA certificate %s", rootCA)
			}

			if ok := certPool.AppendCertsFromPEM(cert); !ok {
				return nil, fmt.Errorf(
					"failed to add root CA certificate %s to the certificate pool",
					rootCA,
				)
			}
		}

		redisOpts = &redis.UniversalOptions{
			Addrs:           addrs,
			Password:        password,
			ReadOnly:        false,
			MaxRetries:      10000,
			MaxRetryBackoff: 20 * time.Second,
			TLSConfig:       &tls.Config{RootCAs: certPool},
		}
	} else {
		redisOpts = &redis.UniversalOptions{
			Addrs:           addrs,
			Password:        password,
			ReadOnly:        false,
			MaxRetries:      10000,
			MaxRetryBackoff: 20 * time.Second,
		}
	}

	var (
		err        error
		connectErr = make(chan error)
		success    = make(chan struct{})
	)

	go func() {
		client = redis.NewUniversalClient(redisOpts)
		if err = client.Ping().Err(); err != nil {
			connectErr <- err
			return
		}
		success <- struct{}{}
	}()

	go func() {
		t := time.NewTicker(20 * time.Second)
		var connHealthy bool
		for range t.C {
			if client == nil {
				log.Warn("Redis client is nil")
				continue
			}

			successPing := make(chan struct{})
			go func() {
				if err = client.Ping().Err(); err != nil {
					connHealthy = false
					return
				}
				successPing <- struct{}{}
			}()
			select {
			case <-time.After(2 * time.Second):
				log.Warn("Redis ping timeout")
				connHealthy = false
			case <-successPing:
				if !connHealthy {
					log.Info("Redis connection restored")
					connHealthy = true
				}
			}
		}
	}()

	select {
	case e := <-connectErr:
		err = fmt.Errorf("failed to ping Redis, error: %s", e)
	case <-time.After(10 * time.Second):
		err = errors.New("failed connect to the Redis")
	case <-success:
	}

	return &Redis{client}, err
}

func (r *Redis) InitChannelsStorage(channels []string) error {
	return nil
}

// Put stores message to Redis List.
func (r *Redis) Put(topic string, msg []byte) error {
	return retry.Do(
		func() error {
			return r.Client.Watch(func(tx *redis.Tx) error {
				return tx.LPush(topic, msg, 0).Err()
			}, topic)
		}, retry.Attempts(300), retry.DelayType(retry.BackOffDelay), retry.MaxDelay(300*time.Millisecond))
}

// Get reads returns one message from the topic.
func (r *Redis) Get(_ context.Context, topic string) ([]byte, error) {
	return r.Client.Get(topic).Bytes()
}

// GetStream is stub.
func (r *Redis) GetStream(ctx context.Context, topic string) (<-chan []byte, <-chan error) {
	return nil, nil
}

// Detele removes item from Redis List.
func (r *Redis) Delete(key string) error {
	return retry.Do(
		func() error {
			return r.Client.Watch(func(tx *redis.Tx) error {
				if err := r.Client.BRPop(0, key).Err(); err != nil {
					return err
				}
				return nil
			}, key)
		}, retry.Attempts(300), retry.DelayType(retry.BackOffDelay), retry.MaxDelay(300*time.Millisecond))
}

// Close closes Redis connection.
func (r *Redis) Close() error {
	return r.Client.Close()
}
