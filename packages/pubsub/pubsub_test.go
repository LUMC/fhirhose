package pubsub

import (
	"fmt"
	"testing"

	"lab.weave.nl/lumc/fhir-hose/packages/pubsub/mocks"
)

func TestMockClient(t *testing.T) {
	// Simply verify the mock conforms the interface
	var register IPubSubClient = &mocks.IPubSubClient{}
	fmt.Println(register)
}
