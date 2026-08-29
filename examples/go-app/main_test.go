package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestCount(t *testing.T) {
	got, err := Count(strings.NewReader("the Quick the  lazy\nTHE quick"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"the": 3, "quick": 2, "lazy": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Count = %v, want %v", got, want)
	}
}

func TestTop(t *testing.T) {
	counts := map[string]int{"b": 2, "a": 2, "c": 5, "d": 1}
	if got, want := Top(counts, 3), []string{"c", "a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Top = %v, want %v", got, want)
	}
	if got := Top(counts, 10); len(got) != 4 {
		t.Fatalf("Top(10) returned %d words, want 4", len(got))
	}
}
