package fhirhose

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// Retrievers creates a retrieve subscriber for each stream
func (c *Register) Retrievers(conf Config, streams []IStream, errChan *chan Error) {
	for _, stream := range streams {
		go handleRetrieve(DefaultConsumerPrefix, stream, conf, errChan)
		go handleRetrieve(DefaultConsumerCustomLoadPrefix, stream, conf, errChan)
	}
}

// handleRetrieve handles the retrieve action
func handleRetrieve(prefix ConsumerPrefix, stream IStream, conf Config, errChan *chan Error) {
	// Consume from polled consumer or given resource
	consumerString := GetConsumeAction(stream.GetStreamName(), prefix, PollAction)
	logrus.WithFields(logrus.Fields{"consumer": consumerString}).Info("register consumer")
	err := conf.PubSub.Consume(consumerString, string(DefaultStreamName), func(msg *nats.Msg) {
		// Retrieve message
		var message StreamMessage
		id := GetIdentifierFromActionString(msg.Subject)
		err := json.Unmarshal(msg.Data, &message)
		if err != nil {
			logrus.WithError(err).Error("can't unmarshal message")
		}

		updatedMessage, funcErr := stream.Retrieve(message)
		if funcErr != nil && errChan != nil {
			*errChan <- Error{
				Event:         stream.GetStreamName(),
				Action:        RetrieveAction,
				StreamMessage: &updatedMessage,
				Error:         funcErr,
			}
		}

		// Acknowledge message received event
		err = msg.Respond(nil)
		if err != nil {
			logrus.WithError(err).Error("can't acknowledge message")
		}

		if funcErr == nil {
			logrus.WithFields(logrus.Fields{
				"id":   id,
				"desc": updatedMessage.Description,
				"time": time.Now(),
			}).Info("retrieved item")

			// Create message bytes
			messageBytes, err := json.Marshal(&updatedMessage)
			if err != nil {
				logrus.WithError(err).Error("can't marshal message bytes")
			}

			// Publish
			actionString := GetPublishAction(message.Identifier, stream.GetStreamName(), DefaultConsumerPrefix, RetrieveAction)
			if err := conf.PubSub.Publish(actionString, messageBytes); err != nil {
				logrus.WithError(err).Error("can't publish new event")
			}
		}
	})
	if err != nil {
		logrus.WithField("consumer", consumerString).WithError(err).Error("can't consume from consumer")
	}
}
