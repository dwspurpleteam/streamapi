package webhook

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"

	"github.com/dwspurpleteam/streamapi/internal/app/events/domain"
	events "github.com/dwspurpleteam/streamapi/internal/app/events/service"
)

var ErrIgnored = errors.New("unsupported webhook event")

type Verifier interface {
	Verify(request *http.Request) (domain.Event, error)
}

type Receiver interface {
	Accept(ctx context.Context, event domain.Event) error
}

type Handler struct {
	verifier Verifier
	receiver Receiver
	maxBytes int64
	slots    chan struct{}
	verified atomic.Uint64
	rejected atomic.Uint64
	ignored  atomic.Uint64
}

func New(
	verifier Verifier,
	receiver Receiver,
	maxBytes int64,
	concurrency int,
) *Handler {
	return &Handler{
		verifier: verifier,
		receiver: receiver,
		maxBytes: maxBytes,
		slots:    make(chan struct{}, concurrency),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)

		return
	}

	select {
	case h.slots <- struct{}{}:
		defer func() {
			<-h.slots
		}()
	default:
		h.rejected.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)

		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.maxBytes)

	event, err := h.verifier.Verify(r)
	if errors.Is(err, ErrIgnored) {
		h.ignored.Add(1)
		w.WriteHeader(http.StatusNoContent)

		return
	}

	if err != nil {
		h.rejected.Add(1)

		if sizeError, ok := errors.AsType[*http.MaxBytesError](err); ok && sizeError != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
		}

		return
	}

	h.verified.Add(1)

	if err := h.receiver.Accept(r.Context(), event); err != nil {
		if errors.Is(err, events.ErrInvalid) {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type Snapshot struct {
	Verified uint64
	Rejected uint64
	Ignored  uint64
}

func (h *Handler) Snapshot() Snapshot {
	return Snapshot{
		Verified: h.verified.Load(),
		Rejected: h.rejected.Load(),
		Ignored:  h.ignored.Load(),
	}
}
