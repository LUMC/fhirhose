package fhirhose

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// Uploaders registers retrieve consumers for each stream
// it also puts processed items into the upload channel when defined
func (c *Register) Uploaders(conf Config, streams []IStream, errChan *chan Error, uploadChan *chan StreamMessage) {
	for _, stream := range streams {
		go handleUpload(DefaultConsumerPrefix, stream, conf, errChan, uploadChan)
		go handleUpload(DefaultConsumerCustomLoadPrefix, stream, conf, errChan, uploadChan)
	}
}

// handleUpload handles the upload action
func handleUpload(prefix ConsumerPrefix, stream IStream, conf Config, errChan *chan Error, uploadChan *chan StreamMessage) {
	// Consume from transformed consumer or given resource
	consumerString := GetConsumeAction(stream.GetStreamName(), prefix, TransformAction)
	logrus.WithFields(logrus.Fields{"consumer": consumerString}).Info("register consumer")
	err := conf.PubSub.Consume(consumerString, string(DefaultStreamName), func(msg *nats.Msg) {
		// Retrieve message
		var message StreamMessage
		id := GetIdentifierFromActionString(msg.Subject)
		err := json.Unmarshal(msg.Data, &message)
		if err != nil {
			logrus.WithError(err).Error("can't unmarshal message")
		}

		updatedMessage, shouldUpload, funcErr := stream.Upload(message)
		if funcErr != nil && errChan != nil {
			*errChan <- Error{
				Event:         stream.GetStreamName(),
				Action:        UploadAction,
				StreamMessage: &updatedMessage,
				Error:         funcErr,
			}
		}

		// Acknowledge message received event
		err = msg.Respond(nil)
		if err != nil {
			logrus.WithError(err).Error("can't acknowledge message")
		}

		if funcErr == nil && shouldUpload {
			logrus.WithFields(logrus.Fields{"id": id, "time": time.Now()}).Info("uploaded item put data into upload channel")
			if uploadChan != nil {
				*uploadChan <- updatedMessage
			} else {
				logrus.WithField("id", id).Warn("can't put data into the upload channel when channel is nil")
			}
		}
	})
	if err != nil {
		logrus.WithField("consumer", consumerString).WithError(err).Error("can't consume from consumer")
	}
}
