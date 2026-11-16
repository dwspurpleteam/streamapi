package domain

import "testing"

func TestSelectionRestrictsMediaSources(t *testing.T) {
	s := Selection{Sources: 1 << 1}
	if !s.Matches(Track{Kind: Audio, Source: 1}) {
		t.Fatal("microphone excluded")
	}

	if s.Matches(Track{Kind: Audio, Source: 4}) {
		t.Fatal("screen audio included")
	}

	if s.Matches(Track{Kind: Audio}) {
		t.Fatal("unknown source included")
	}
}
