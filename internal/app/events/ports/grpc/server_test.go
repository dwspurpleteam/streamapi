package grpc

import "testing"

func TestEventTypeForSubscriber(t *testing.T) {
	const (
		published         = "media.track_published"
		unpublished       = "media.track_unpublished"
		legacyPublished   = "track.published"
		legacyUnpublished = "track.unpublished"
	)

	for _, tc := range []struct {
		name      string
		requested []string
		want      string
	}{
		{legacyPublished, []string{published}, published},
		{published, []string{legacyPublished}, legacyPublished},
		{legacyUnpublished, []string{unpublished}, unpublished},
		{unpublished, []string{legacyUnpublished}, legacyUnpublished},
		{legacyPublished, nil, published},
		{"participant.joined", nil, "participant.joined"},
	} {
		if got := eventTypeForSubscriber(tc.name, tc.requested); got != tc.want {
			t.Errorf("event type %q, requested %v: got %q, want %q", tc.name, tc.requested, got, tc.want)
		}
	}
}
