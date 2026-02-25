//go:build linux

package filesystem

import "testing"

func TestFuseConfigAllowsOther(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		want     bool
	}{
		{
			name: "enabled explicit line",
			contents: `
# comment
user_allow_other
`,
			want: true,
		},
		{
			name: "enabled with trailing tokens",
			contents: `
user_allow_other  # enabled
`,
			want: true,
		},
		{
			name: "commented out",
			contents: `
#user_allow_other
`,
			want: false,
		},
		{
			name: "missing",
			contents: `
# nothing relevant
`,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fuseConfigAllowsOther(tt.contents)
			if got != tt.want {
				t.Fatalf("fuseConfigAllowsOther() = %v, want %v", got, tt.want)
			}
		})
	}
}
