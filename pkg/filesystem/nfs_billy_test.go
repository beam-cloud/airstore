package filesystem

import "testing"

func TestBillyFSCleanPathNormalizesRoot(t *testing.T) {
	b := &billyFS{}

	cases := []struct {
		in   string
		want string
	}{
		{in: "", want: "/"},
		{in: ".", want: "/"},
		{in: "/", want: "/"},
		{in: "/.", want: "/"},
		{in: "tools", want: "/tools"},
		{in: "tools/../memory", want: "/memory"},
	}

	for _, tc := range cases {
		if got := b.cleanPath(tc.in); got != tc.want {
			t.Fatalf("cleanPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestBillyFSJoinHandlesRoot(t *testing.T) {
	b := &billyFS{}

	if got := b.Join(); got != "/" {
		t.Fatalf("Join() = %q, want /", got)
	}
	if got := b.Join("."); got != "/" {
		t.Fatalf("Join(\".\") = %q, want /", got)
	}
	if got := b.Join("tools", "bin"); got != "tools/bin" {
		t.Fatalf("Join(\"tools\", \"bin\") = %q, want tools/bin", got)
	}
}
