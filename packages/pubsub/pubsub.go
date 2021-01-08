// Package pubsub is a nats wrapper with additional mock client
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// IPubSubClient wrapper of github.com/nats-io/nats.go client
type IPubSubClient interface {
	Publish(subj string, data []byte) error
	Subscribe(subj string, cb nats.MsgHandler) (*nats.Subscription, error)
	Consume(consumer, stream string, cb func(msg *nats.Msg)) error
}

// Client struct
type Client struct {
	Conn *nats.Conn
}

// Publish wrapper for connection publish function
func (p *Client) Publish(topic string, data []byte) error {
	return p.Conn.Publish(topic, data)
}

// Consume creates a new consumer connection ands start listing for messages
// every message is handled by the given callback parameter
func (p *Client) Consume(consumer, stream string, callback func(msg *nats.Msg)) (err error) {
	// Create new connection for every consumer
	// We do this because every consumer connection is blocking
	consumerConn, err := nats.Connect(p.Conn.ConnectedAddr())
	if err != nil {
		return fmt.Errorf("connecting to nats server failed: %w", err)
	}

	// Create new manager for consumer connection
	manager, err := jsm.New(consumerConn, jsm.WithTimeout(time.Hour*1))
	if err != nil {
		return fmt.Errorf("creating new manager failed: %w", err)
	}

	// Load active consumer from manager
	activeConsumer, err := manager.LoadConsumer(stream, consumer)
	if err != nil {
		return fmt.Errorf("loading consumer %s for stream %s failed: %w", consumer, stream, err)
	}

	// Poll messages on the active consumer
	for {
		msg, err := activeConsumer.NextMsg()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Handle timeout by returning new consumer
				logrus.Warn("connection deadline exceeded, returning fresh consumer with new connection")
				err := consumerConn.Drain()
				if err != nil {
					return fmt.Errorf("draining timed out consumer connection failed: %w", err)
				}
				return p.Consume(consumer, stream, callback)
			}
			return fmt.Errorf("uknown error from active consumer: %w", err)
		}

		// Handle incoming messages with callback
		callback(msg)
	}
}

// Subscribe wrapper for connection subscribe function
func (p *Client) Subscribe(topic string, callback nats.MsgHandler) (subscription *nats.Subscription, err error) {
	s, err := p.Conn.Subscribe(topic, func(msg *nats.Msg) {
		callback(msg)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribing to topic failed: %w", err)
	}
	return s, nil
}
