package main

import (
	"strings"
	"testing"
)

func TestAdministrativeCommandArguments(t *testing.T) {
	for _, args := range [][]string{{"replay"}, {"reconcile"}, {"activate"}, {"retry", "--id", "source"}, {"unknown"}, {"prepare", "unexpected"}} {
		if _, err := parseCommand(args); err == nil {
			t.Fatal("invalid administrative arguments accepted", args)
		}
	}
	for _, args := range [][]string{{"prepare"}, {"drain"}, {"replay", "--input", "source.jsonl"}, {"reconcile", "--input", "authority.jsonl"}, {"activate", "--report", "report", "--expected-revision", "2"}, {"inspect", "--id", "source"}, {"retry", "--id", "source", "--expected-digest", strings.Repeat("a", 64), "--reason", "ticket-123"}} {
		if _, err := parseCommand(args); err != nil {
			t.Fatal(args, err)
		}
	}
}
