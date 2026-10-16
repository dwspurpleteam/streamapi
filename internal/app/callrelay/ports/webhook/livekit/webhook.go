package livekit

import (
	"fmt"
	"maps"
	"net/http"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/webhook"

	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/domain"
	webhookport "github.com/dwspurpleteam/callrelay/internal/app/callrelay/ports/webhook"
)

type Verifier struct {
	keys   *auth.SimpleKeyProvider
	source string
	types  map[string]string
}

func NewVerifier(key, secret, source string, types map[string]string) *Verifier {
	return &Verifier{
		keys:   auth.NewSimpleKeyProvider(key, secret),
		source: source,
		types:  maps.Clone(types),
	}
}

func (v *Verifier) Verify(r *http.Request) (domain.Event, error) {
	e, err := webhook.ReceiveWebhookEvent(r, v.keys)
	if err != nil {
		return domain.Event{}, fmt.Errorf("verify platform webhook: %w", err)
	}

	t := v.types[e.GetEvent()]
	if t == "" {
		return domain.Event{}, webhookport.ErrIgnored
	}

	return domain.Event{
		ID:     e.GetId(),
		Type:   t,
		Source: v.source,
		CallID: e.GetRoom().GetSid(),
		Room:   e.GetRoom().GetName(),

		Participant: e.GetParticipant().GetIdentity(),
		Track:       e.GetTrack().GetSid(),
		OccurredAt:  time.Unix(e.GetCreatedAt(), 0),
	}, nil
}
