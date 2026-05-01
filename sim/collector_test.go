package sim

import (
	"reflect"
	"testing"
)

func TestNodeEventDoesNotCarryPayloadData(t *testing.T) {
	if _, ok := reflect.TypeOf(NodeEvent{}).FieldByName("Data"); ok {
		t.Fatal("NodeEvent must remain metadata-only; payload data is retained by buffered event channels")
	}
}
