package consulstructure

import (
	"reflect"
	"testing"
	"time"

	consul "github.com/hashicorp/consul/api"
)

func TestDecode_basic(t *testing.T) {
	type test struct {
		Addr string
	}

	// Write our test data
	defer testClientWrite(t, testClient(t), map[string]string{
		"test/addr": "foo",
	})()

	updateCh := make(chan interface{})
	errCh := make(chan error)
	d := &Decoder{
		Target:   &test{},
		Prefix:   "test/",
		UpdateCh: updateCh,
	}
	defer d.Close()
	go d.Run()

	var raw interface{}
	select {
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	case err := <-errCh:
		t.Fatalf("err: %s", err)
	case raw = <-updateCh:
	}

	expected := &test{Addr: "foo"}
	actual := raw.(*test)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %#v", actual)
	}
}

func TestDecode_emptyPrefix(t *testing.T) {
	type test struct {
		Addr string
	}

	updateCh := make(chan interface{})
	errCh := make(chan error)
	d := &Decoder{
		Target:   &test{},
		UpdateCh: updateCh,
		ErrCh:    errCh,
	}
	defer d.Close()
	go d.Run()

	select {
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	case <-errCh:
		return
	case v := <-updateCh:
		t.Fatalf("got update: %#v", v)
	}
}

func TestDecode_nested(t *testing.T) {
	type testChild struct {
		Data string
	}

	type test struct {
		Addr  string
		Child testChild
	}

	// Write our test data
	defer testClientWrite(t, testClient(t), map[string]string{
		"test/addr":       "foo",
		"test/child/data": "bar",
	})()

	updateCh := make(chan interface{})
	errCh := make(chan error)
	d := &Decoder{
		Target:   &test{},
		Prefix:   "test/",
		UpdateCh: updateCh,
	}
	defer d.Close()
	go d.Run()

	var raw interface{}
	select {
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	case err := <-errCh:
		t.Fatalf("err: %s", err)
	case raw = <-updateCh:
	}

	expected := &test{
		Addr:  "foo",
		Child: testChild{Data: "bar"},
	}
	actual := raw.(*test)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %#v", actual)
	}
}

func TestDecode_tag(t *testing.T) {
	type test struct {
		Addr string `consul:"other"`
	}

	// Write our test data
	defer testClientWrite(t, testClient(t), map[string]string{
		"test/other": "foo",
	})()

	updateCh := make(chan interface{})
	errCh := make(chan error)
	d := &Decoder{
		Target:   &test{},
		Prefix:   "test/",
		UpdateCh: updateCh,
	}
	defer d.Close()
	go d.Run()

	var raw interface{}
	select {
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	case err := <-errCh:
		t.Fatalf("err: %s", err)
	case raw = <-updateCh:
	}

	expected := &test{Addr: "foo"}
	actual := raw.(*test)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %#v", actual)
	}
}

func testClient(t *testing.T) *consul.Client {
	client, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	if _, err := client.Status().Leader(); err != nil {
		t.Fatalf("error requesting Consul leader. Is Consul running?\n\n%s", err)
	}

	return client
}

func testClientWrite(t *testing.T, client *consul.Client, data map[string]string) func() {
	for k, v := range data {
		_, err := client.KV().Put(&consul.KVPair{
			Key:   k,
			Value: []byte(v),
		}, nil)
		if err != nil {
			t.Fatalf("error writing to Consul: %s", err)
		}
	}

	return func() {
		for k, _ := range data {
			_, err := client.KV().Delete(k, nil)
			if err != nil {
				t.Fatalf("error deleting from Consul: %s", err)
			}
		}
	}
}
