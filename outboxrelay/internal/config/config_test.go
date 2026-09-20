package config

import "testing"

func TestParseTopicMap(t *testing.T) {
	cases := []struct {
		input   string
		want    map[string]string
		wantErr bool
	}{
		{input: ""},
		{input: "ledger-events=ledger.events.v1", want: map[string]string{"ledger-events": "ledger.events.v1"}},
		{input: "old=a, other=b", want: map[string]string{"old": "a", "other": "b"}},
		{input: "old", wantErr: true},
		{input: "=new", wantErr: true},
		{input: "old=", wantErr: true},
		{input: "old=new,old=other", wantErr: true},
		{input: "old=new,", wantErr: true},
		{input: "old=new=extra", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseTopicMap(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("map=%v, want=%v", got, tc.want)
			}
			for source, destination := range tc.want {
				if got[source] != destination {
					t.Fatalf("map=%v, want=%v", got, tc.want)
				}
			}
		})
	}
}
