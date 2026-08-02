package nacos

import "testing"

func TestSplitDataIds(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"app.json", []string{"app.json"}},
		{"app.json,db.json", []string{"app.json", "db.json"}},
		{"app.json,,db.json,", []string{"app.json", "db.json"}}, // empty segments skipped
		{",", nil},
	}
	for _, tt := range tests {
		got := splitDataIds(tt.in)
		if len(got) != len(tt.want) {
			t.Errorf("splitDataIds(%q) = %v, want %v", tt.in, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitDataIds(%q)[%d] = %q, want %q", tt.in, i, got[i], tt.want[i])
			}
		}
	}
}
