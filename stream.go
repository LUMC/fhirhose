package fhirhose

import (
	"fmt"
	"strings"
)

// StreamMessage contains all the information
// needed to pass data between stream functions
type StreamMessage struct {
	Identifier  string
	Description string
	Data        []byte
}

// IStream interface containing stream functions
type IStream interface {
	GetStreamName() (streamName StreamName)
	Poll() (inputMessages []StreamMessage, customLoad bool, outputError error)
	Retrieve(inputMessage StreamMessage) (outputMessage StreamMessage, error error)
	Transform(inputMessage StreamMessage) (outputMessage StreamMessage, error error)
	Upload(inputMessage StreamMessage) (outputMessage StreamMessage, shouldUpload bool, error error)
}

// GetPublishAction create consumer string based on identifier, prefix stream and action
func GetPublishAction(identifier string, stream StreamName, prefix ConsumerPrefix, action ActionName) string {
	return fmt.Sprintf("%s.%s.%s.%s", prefix, stream, action, identifier)
}

// GetConsumeAction create consumer string based on prefix stream and action
func GetConsumeAction(stream StreamName, prefix ConsumerPrefix, action ActionName) string {
	return fmt.Sprintf("%s-%s-%s", prefix, stream, action)
}

// GetIdentifierFromActionString extract identifier from action string
func GetIdentifierFromActionString(actionString string) string {
	parts := strings.Split(actionString, ".")
	return parts[len(parts)-1:][0]
}
