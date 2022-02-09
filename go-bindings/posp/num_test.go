package posp

import (
	"reflect"
	"testing"
)

func TestNum(t *testing.T) {
	num := New()
	num.Inc()
	if num.GetValue() != 2 {
		t.Error("unexpected value received")
	}
	num.Inc()
	num.Inc()
	num.Inc()
	if num.GetValue() != 5 {
		t.Error("unexpected value received")
	}
	value := num.GetValue()
	num.Free()

	typ := reflect.TypeOf(value)
	if typ.Name() != "int" {
		t.Error("got unexpected type")
	}
}
