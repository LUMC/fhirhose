package fhirhose

import (
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
)

// Pollers runs all polls for the registered streams based on an time interval
func (c *Register) Pollers(conf Config, streams []IStream, errChan *chan Error) {
	for _, stream := range streams {
		go func(stream IStream) {
			pollOnInterval(conf, stream, errChan)
		}(stream)
	}
}

// pollOnInterval runs a poller for a stream
func pollOnInterval(conf Config, stream IStream, errChan *chan Error) {
	ticker := time.NewTicker(conf.PollInterval)
	done := make(chan bool)

	logrus.WithFields(logrus.Fields{
		"resource": stream.GetStreamName(),
	}).Infof("starting poll in %v", conf.PollInterval)

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			messages, customLoad, err := stream.Poll()
			if err != nil {
				if errChan != nil {
					*errChan <- Error{
						Event:         stream.GetStreamName(),
						Action:        PollAction,
						StreamMessage: nil,
						Error:         err,
					}
				}
			}

			if conf.DeduplicationEnabled {
				messages = deduplicateIdentifiers(messages)
			}

			logrus.WithFields(logrus.Fields{
				"resource": stream.GetStreamName(),
				"changes":  len(messages),
			}).Info("polled")

			for _, message := range messages {
				logrus.WithFields(logrus.Fields{
					"id":   message.Identifier,
					"desc": message.Description,
					"time": time.Now(),
				}).Info("polled item")

				messageBytes, err := json.Marshal(&message)
				if err != nil {
					logrus.WithError(err).Error("can't marshal message bytes")
				}

				prefix := DefaultConsumerPrefix
				if customLoad {
					prefix = DefaultConsumerCustomLoadPrefix
				}

				actionString := GetPublishAction(message.Identifier, stream.GetStreamName(), prefix, PollAction)
				if err := conf.PubSub.Publish(actionString, messageBytes); err != nil {
					logrus.WithError(err).Error("can't publish new event")
				}
				if conf.ThrottleAmount != nil {
					time.Sleep(time.Second / time.Duration(*conf.ThrottleAmount))
				}
			}
		}
	}
}

// deduplicateIdentifiers removes duplicate identifiers from the list
func deduplicateIdentifiers(messages []StreamMessage) []StreamMessage {
	messagesByIdentifier := make(map[string]StreamMessage)
	var deduplicatedMessages []StreamMessage
	for _, m := range messages {
		_, found := messagesByIdentifier[m.Identifier]
		if !found {
			messagesByIdentifier[m.Identifier] = m
			deduplicatedMessages = append(deduplicatedMessages, m)
		}
	}

	return deduplicatedMessages
}
