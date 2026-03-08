package bamlregistry

import (
	"reflect"
	"sync"

	baml "github.com/boundaryml/baml/engine/language_client_go/pkg"
)

var (
	mu        sync.Mutex
	mergedMap = map[string]reflect.Type{}
)

// RegisterTypeMap merges a generated BAML client's type map into the single
// global BAML runtime type map. The upstream Go runtime stores this globally,
// so multiple generated clients must cooperate instead of overwriting each
// other during init().
func RegisterTypeMap(typeMap map[string]reflect.Type) {
	mu.Lock()
	defer mu.Unlock()

	for key, value := range typeMap {
		mergedMap[key] = value
	}

	snapshot := make(map[string]reflect.Type, len(mergedMap))
	for key, value := range mergedMap {
		snapshot[key] = value
	}

	baml.SetTypeMap(snapshot)
}
