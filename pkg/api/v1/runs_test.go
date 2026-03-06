package apiv1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsSimpleListRunsQuery(t *testing.T) {
	require.True(t, isSimpleListRunsQuery("", "", "", "", "", "", "", "", ""))
	require.False(t, isSimpleListRunsQuery("", "", "", "", "", "", "", "2026-03-05T00:00:00Z", ""))
	require.False(t, isSimpleListRunsQuery("", "", "", "", "", "", "", "", "2026-03-05T00:00:00Z"))
}
