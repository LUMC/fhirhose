package fhirhose

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	psmocks "github.com/lumc/fhirhose/packages/pubsub/mocks"
)

// FhirhoseTestSuite tests for the fhirhose package
type FhirhoseTestSuite struct {
	suite.Suite
	client *Client
}

func (s *FhirhoseTestSuite) SetupTest() {
	mockedPubSub := &psmocks.IPubSubClient{}

	mockedPubSub.On("Publish", mock.Anything, mock.Anything).Return(nil)
	mockedPubSub.On("Subscribe", mock.Anything, mock.Anything).Return(&nats.Subscription{}, nil)
	mockedPubSub.On("Consume", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	client := &Client{
		Config: &Config{
			PubSub:               mockedPubSub,
			DeduplicationEnabled: true,
			PollInterval:         time.Second * 5,
		},
		Register: &Register{},
		Streams:  nil,
	}

	s.client = client
}

func (s *FhirhoseTestSuite) TestMocks() {
	// Simply verify the mock conforms the interface
	var register IRegister = &IRegisterMock{}
	s.NotNil(register)

	var stream IStream = &IStreamMock{}
	s.NotNil(stream)
}

func (s *FhirhoseTestSuite) TestErrorEmptyStreams() {
	s.Require().Nil(s.client.Streams, "this test case doesn't expect client streams")
	err := s.client.Run()
	s.Require().Error(err, "expect error when client streams are nil")
	s.Require().EqualError(err, ErrNoStreams.Error())
}

func (s *FhirhoseTestSuite) TestErrorDuplicateStreams() {
	userStream := IStreamMock{}
	userStream.On("GetStreamName").Return(StreamName("user"))

	otherUserStream := IStreamMock{}
	otherUserStream.On("GetStreamName").Return(StreamName("user"))

	s.client.Streams = []IStream{
		&userStream,
		&otherUserStream,
	}

	s.Require().NotNil(s.client.Streams, "this test case expect client streams")
	err := s.client.Run()
	s.Require().Error(err, "expect error for duplicate streams")
	s.Require().EqualError(err, ErrDuplicateStream.Error())
}

func (s *FhirhoseTestSuite) TestRegisterStreamsPollDisabled() {
	// Create mocked Register
	mockedRegister := &IRegisterMock{}
	userStream := IStreamMock{}
	userStream.On("GetStreamName").Return(StreamName("user"))

	carStream := IStreamMock{}
	carStream.On("GetStreamName").Return(StreamName("car"))

	var wgRetrieve sync.WaitGroup
	var wgTransform sync.WaitGroup
	var wgUpdate sync.WaitGroup

	// Check if Register functions get called
	mockedRegister.On("Uploaders", mock.Anything, mock.Anything, mock.Anything).Return().Run(func(args mock.Arguments) {
		wgUpdate.Done()
	})
	mockedRegister.On("Transformers", mock.Anything, mock.Anything, mock.Anything).Return().Run(func(args mock.Arguments) {
		wgTransform.Done()
	})
	mockedRegister.On("Retrievers", mock.Anything, mock.Anything, mock.Anything).Return().Run(func(args mock.Arguments) {
		wgRetrieve.Done()
	})

	s.client.Register = mockedRegister
	s.client.Streams = []IStream{
		&carStream,
		&userStream,
	}

	s.Require().NotNil(s.client.Streams, "this test case expect client streams")

	wgRetrieve.Add(1)
	wgTransform.Add(1)
	wgUpdate.Add(1)

	s.client.Config.PollEnabled = false
	s.NoError(s.client.Run())

	wgRetrieve.Wait()
	wgTransform.Wait()
	wgUpdate.Wait()

	// Assert that the send email function is called
	mockedRegister.AssertNumberOfCalls(s.T(), "Uploaders", 1)
	mockedRegister.AssertNumberOfCalls(s.T(), "Transformers", 1)
	mockedRegister.AssertNumberOfCalls(s.T(), "Retrievers", 1)
}

func (s *FhirhoseTestSuite) TestRegisterStreamsPollEnabled() {
	// Create mocked Register
	mockedRegister := &IRegisterMock{}
	userStream := IStreamMock{}
	userStream.On("GetStreamName").Return(StreamName("user"))

	carStream := IStreamMock{}
	carStream.On("GetStreamName").Return(StreamName("car"))

	var wgPoll sync.WaitGroup

	// Check if Register functions get called
	mockedRegister.On("Pollers", mock.Anything, mock.Anything, mock.Anything).Return().Run(func(args mock.Arguments) {
		wgPoll.Done()
	})

	s.client.Register = mockedRegister
	s.client.Streams = []IStream{
		&carStream,
		&userStream,
	}

	s.Require().NotNil(s.client.Streams, "this test case expect client streams")

	wgPoll.Add(1)

	s.client.Config.PollEnabled = true
	s.NoError(s.client.Run())

	wgPoll.Wait()

	// Assert that the send email function is called
	mockedRegister.AssertNumberOfCalls(s.T(), "Pollers", 1)
}

func TestFhirhoseTestSuite(t *testing.T) {
	suite.Run(t, new(FhirhoseTestSuite))
}
