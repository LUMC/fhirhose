package pubsub

import (
	"fmt"
	"testing"

	"github.com/lumc/fhirhose/packages/pubsub/mocks"
)

func TestMockClient(t *testing.T) {
	// Simply verify the mock conforms the interface
	var register IPubSubClient = &mocks.IPubSubClient{}
	fmt.Println(register)
}
