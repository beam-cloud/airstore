package services

import (
	"fmt"
	"testing"
)

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("NoSuchKey: The specified key does not exist"), true},
		{fmt.Errorf("NotFound: resource not available"), true},
		{fmt.Errorf("InvalidRange: The requested range is not satisfiable"), false},
		{fmt.Errorf("timeout"), false},
	}
	for _, tt := range tests {
		if got := isNotFound(tt.err); got != tt.want {
			t.Errorf("isNotFound(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestIsInvalidRange(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("InvalidRange: The requested range is not satisfiable"), true},
		{fmt.Errorf("NoSuchKey: The specified key does not exist"), false},
		{fmt.Errorf("NotFound: resource"), false},
		{fmt.Errorf("timeout"), false},
	}
	for _, tt := range tests {
		if got := isInvalidRange(tt.err); got != tt.want {
			t.Errorf("isInvalidRange(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
