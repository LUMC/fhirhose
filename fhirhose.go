// Package fhirhose contains all logic to create a nats streaming pipeline based on a resource combined with implemented poll, retrieve, transform and insert functions
package fhirhose

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/lumc/fhirhose/packages/pubsub"
)

type (
	// StreamName description of resource name
	StreamName string
	// ActionName description of action being called
	ActionName string
	// ConsumerPrefix prefix of the consumer address
	ConsumerPrefix string
)

var (
	// ErrNoStreams err returned when no streams are supplied in new client
	ErrNoStreams = errors.New("streams can't be empty")
	// ErrDuplicateStream err returned when duplicate streams are found in streams array
	ErrDuplicateStream = errors.New("duplicate stream found")
	// ErrValidateStream err returned when validating stream object failed
	ErrValidateStream = errors.New("validating stream failed")
)

const (
	// DefaultStreamName default stream name
	DefaultStreamName StreamName = "fhirhose"
	// DefaultConsumerPrefix default consumer prefix
	DefaultConsumerPrefix ConsumerPrefix = "fhirhose"
	// DefaultConsumerPrefix default custom load consumer prefix
	DefaultConsumerCustomLoadPrefix ConsumerPrefix = "fhirhosecl"
	// PollAction event use as base poll streaming subject
	PollAction ActionName = "polled"
	// RetrieveAction event use as base retrieve streaming subject
	RetrieveAction ActionName = "retrieved"
	// TransformAction event use as base transform streaming subject
	TransformAction ActionName = "transformed"
	// UploadAction event use as base put streaming subject
	UploadAction ActionName = "uploaded"
)

// Client type
type Client struct {
	Config         *Config
	Streams        []IStream
	Register       IRegister
	ErrorCallback  *ErrHandlerFunc
	UploadCallback *UploadHandlerFunc
	errorChannel   *chan Error
	uploadChannel  *chan StreamMessage
}

// UploadHandlerFunc func used in callback for uploads handling
type UploadHandlerFunc func(uploads []StreamMessage) error

// ErrHandlerFunc func used in callback for error handling
type ErrHandlerFunc func(error Error)

// Error custom error type used in error channel
type Error struct {
	StreamMessage *StreamMessage
	Action        ActionName
	Event         StreamName
	Error         error
}

// IRegister interface containing register functions
type IRegister interface {
	Retrievers(Config, []IStream, *chan Error)
	Transformers(Config, []IStream, *chan Error)
	Uploaders(Config, []IStream, *chan Error, *chan StreamMessage)
	Pollers(Config, []IStream, *chan Error)
}

// Register struct found on client
type Register struct{}

// NewClient creates a new fhirhose client
func NewClient(config Config, streams []IStream, errorCallback *ErrHandlerFunc, uploadCallback *UploadHandlerFunc) *Client {
	c := &Client{
		Config:   &config,
		Streams:  streams,
		Register: &Register{},
	}

	// Set err channel when error callback is defined
	if errorCallback != nil {
		c.ErrorCallback = errorCallback
		errChan := make(chan Error)
		c.errorChannel = &errChan
	}

	// Set upload channel when upload callback is defined
	if uploadCallback != nil {
		c.UploadCallback = uploadCallback
		uploadChan := make(chan StreamMessage)
		c.uploadChannel = &uploadChan
	}

	if config.UploadBatchSize == 0 {
		config.UploadBatchSize = 50
	}

	return c
}

// Run runs all the streams
func (c *Client) Run() error {
	if c.Streams == nil {
		return ErrNoStreams
	}

	registered := make(map[StreamName]bool)

	// Validate all before running the streams
	for _, stream := range c.Streams {
		if _, ok := registered[stream.GetStreamName()]; ok {
			return ErrDuplicateStream
		}
		registered[stream.GetStreamName()] = true
	}

	// Run streams
	// Register subscribers when poll is disabled in config
	// Register subscribers for transformers
	for i := 0; i < c.Config.WorkerAmount; i++ {
		c.Register.Transformers(*c.Config, c.Streams, c.errorChannel)
	}

	// Register subscribers for updaters
	for i := 0; i < c.Config.WorkerAmount; i++ {
		c.Register.Uploaders(*c.Config, c.Streams, c.errorChannel, c.uploadChannel)
	}

	for i := 0; i < c.Config.WorkerAmount; i++ {
		go c.Register.Retrievers(*c.Config, c.Streams, c.errorChannel)
	}

	// Run pollers when enabled in config
	c.Register.Pollers(*c.Config, c.Streams, c.errorChannel)

	// Push error channels into error callback when defined
	if c.ErrorCallback != nil {
		handlerFunc := *c.ErrorCallback
		// Example of error handling from channel
		go func() {
			for {
				err, ok := <-*c.errorChannel
				if ok {
					handlerFunc(err)
				}
			}
		}()
	}

	// Push uploads to uploads handler when batch size is reached
	if c.UploadCallback != nil {
		uploadFunc := *c.UploadCallback
		go func() {
			var uploadBatch []StreamMessage
			lastUpload := time.Now()
			for {
				uploadItem, ok := <-*c.uploadChannel
				if ok {
					uploadBatch = append(uploadBatch, uploadItem)
					batchSize := len(uploadBatch)
					if batchSize > c.Config.UploadBatchSize || batchSize > 0 && time.Now().Sub(lastUpload) > time.Second*1 {
						payload := uploadBatch
						if err := uploadFunc(payload); err != nil && c.errorChannel != nil {
							*c.errorChannel <- Error{
								Event:         "upload handler func",
								Action:        UploadAction,
								StreamMessage: nil,
								Error:         err,
							}
						}
						uploadBatch = []StreamMessage{}
						lastUpload = time.Now()
					}
				}
			}
		}()
	}

	return nil
}

// Config type is the base config struct for the fhirhose package
type Config struct {
	PubSub               pubsub.IPubSubClient
	Nats                 *nats.Conn
	PollInterval         time.Duration
	DeduplicationEnabled bool
	// WorkerAmount amount of processes run for retrieve, transform and upload
	// Default 3
	WorkerAmount int
	// ThrottleAmount amount of items pushed per minute
	// Default nil
	ThrottleAmount *int64
	// UploadBatchSize batch size for upload messages
	UploadBatchSize int
}
