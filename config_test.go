package timecache

import (
	"testing"
)

func TestCovertToBytes(t *testing.T) {
	tests := []struct {
		a    string
		want int64
	}{
		{
			a:    "100000",
			want: 100000,
		},
		{
			a:    "100009",
			want: 100009,
		},
		{
			a:    "1KiB",
			want: 1024,
		},
		{
			a:    "1KB",
			want: 1000,
		},
		{
			a:    "1MiB",
			want: 1048576,
		},
		{
			a:    "1MB",
			want: 1000000,
		},
		{
			a:    "1GiB",
			want: 1073741824,
		},
		{
			a:    "1GB",
			want: 1000000000,
		},
	}

	for _, test := range tests {
		got, err := convertToBytes(test.a)
		if err != nil {
			t.Errorf("convertToBytes(%s) = %d, want %d, error: %s", test.a, got, test.want, err)
		}
	}
}
