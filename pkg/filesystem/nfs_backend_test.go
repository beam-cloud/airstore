package filesystem

import "testing"

func TestIsNFSHelperMissingMessage(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want bool
	}{
		{
			name: "reported bad option helper message",
			msg:  "bad option; for several filesystems (e.g. nfs, cifs) you might need a /sbin/mount.<type> helper program.",
			want: true,
		},
		{
			name: "explicit mount.nfs missing",
			msg:  "mount: /mnt: mount.nfs: command not found",
			want: true,
		},
		{
			name: "unrelated mount failure",
			msg:  "permission denied",
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isNFSHelperMissingMessage(tc.msg)
			if got != tc.want {
				t.Fatalf("expected %v, got %v for message: %q", tc.want, got, tc.msg)
			}
		})
	}
}
