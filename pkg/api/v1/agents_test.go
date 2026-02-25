package apiv1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeStrictJSONRejectsUnknownFields(t *testing.T) {
	type payload struct {
		Message string `json:"message"`
	}

	err := decodeStrictJSON([]byte(`{"message":"hello","unknown":"field"}`), &payload{})
	require.Error(t, err)
}

func TestDecodeStrictJSONAcceptsKnownFields(t *testing.T) {
	type payload struct {
		Message string `json:"message"`
	}

	var out payload
	err := decodeStrictJSON([]byte(`{"message":"hello"}`), &out)
	require.NoError(t, err)
	require.Equal(t, "hello", out.Message)
}

func TestDecodeStrictJSONRejectsTrailingJSONValue(t *testing.T) {
	type payload struct {
		Message string `json:"message"`
	}

	err := decodeStrictJSON([]byte(`{"message":"hello"}{"message":"again"}`), &payload{})
	require.Error(t, err)
}

func TestDecodeStrictJSONRejectsTrailingGarbage(t *testing.T) {
	type payload struct {
		Message string `json:"message"`
	}

	err := decodeStrictJSON([]byte(`{"message":"hello"}not-json`), &payload{})
	require.Error(t, err)
}
