package kv

import "testing"

func TestMemStore(t *testing.T) {
	s := NewMemStore()
	if _, ok, _ := s.Get("x"); ok {
		t.Fatal("unexpected key")
	}
	_ = s.Put("x", []byte("5"))
	if v, ok, _ := s.Get("x"); !ok || string(v) != "5" {
		t.Fatalf("got %q %v", v, ok)
	}
	_ = s.Delete("x")
	if _, ok, _ := s.Get("x"); ok {
		t.Fatal("key not deleted")
	}
}
