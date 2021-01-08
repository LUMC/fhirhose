package fhirhose

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// Transformers creates a transform subscriber for each stream
func (c *Register) Transformers(conf Config, streams []IStream, errChan *chan Error) {
	for _, stream := range streams {
		go handleTransform(DefaultConsumerPrefix, stream, conf, errChan)
		go handleTransform(DefaultConsumerCustomLoadPrefix, stream, conf, errChan)
	}
}

// handleTransform handles the transform action
func handleTransform(prefix ConsumerPrefix, stream IStream, conf Config, errChan *chan Error) {
	// Consume from retrieved consumer or given resource
	consumerString := GetConsumeAction(stream.GetStreamName(), prefix, RetrieveAction)
	logrus.WithFields(logrus.Fields{"consumer": consumerString}).Info("register consumer")
	err := conf.PubSub.Consume(consumerString, string(DefaultStreamName), func(msg *nats.Msg) {
		// Transform message
		var message StreamMessage
		id := GetIdentifierFromActionString(msg.Subject)
		err := json.Unmarshal(msg.Data, &message)
		if err != nil {
			logrus.WithError(err).Error("can't unmarshal message")
		}

		updatedMessage, funcErr := stream.Transform(message)
		if funcErr != nil && errChan != nil {
			*errChan <- Error{
				Event:         stream.GetStreamName(),
				Action:        TransformAction,
				StreamMessage: &updatedMessage,
				Error:         funcErr,
			}
		}

		// Acknowledge message transformed event
		err = msg.Respond(nil)
		if err != nil {
			logrus.WithError(err).Error("can't acknowledge message")
		}

		// Publish message transformed event

		if funcErr == nil {
			logrus.WithFields(logrus.Fields{
				"id":   id,
				"desc": updatedMessage.Description,
				"time": time.Now(),
			}).Info("transformed item")

			// Create message bytes
			messageBytes, err := json.Marshal(&updatedMessage)
			if err != nil {
				logrus.WithError(err).Error("can't marshal message bytes")
			}

			// Publish
			actionString := GetPublishAction(message.Identifier, stream.GetStreamName(), DefaultConsumerPrefix, TransformAction)
			if err := conf.PubSub.Publish(actionString, messageBytes); err != nil {
				logrus.WithError(err).Error("can't publish new event")
			}
		}
	})
	if err != nil {
		logrus.WithField("consumer", consumerString).WithError(err).Error("can't consume from consumer")
	}
}
