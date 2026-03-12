package bamlregistry

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterTypeMapMergesEntries(t *testing.T) {
	mu.Lock()
	original := mergedMap
	mergedMap = map[string]reflect.Type{}
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		mergedMap = original
		mu.Unlock()
	})

	RegisterTypeMap(map[string]reflect.Type{
		"TYPES.Foo": reflect.TypeOf(""),
	})
	RegisterTypeMap(map[string]reflect.Type{
		"TYPES.Bar": reflect.TypeOf(int64(0)),
	})

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, mergedMap, 2)
	require.Contains(t, mergedMap, "TYPES.Foo")
	require.Contains(t, mergedMap, "TYPES.Bar")
}
