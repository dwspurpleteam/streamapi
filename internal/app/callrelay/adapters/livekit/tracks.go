package livekit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"

	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/domain"
	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/service/media"
)

const rtpHeaderBytes = 12

type trackReaders struct {
	source  *Source
	failure chan<- error
	mu      sync.Mutex
	wg      sync.WaitGroup
	closing bool
	active  atomic.Int64
	handles atomic.Uint32
}

func (r *trackReaders) subscribe(ctx context.Context, track *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication, participant *lksdk.RemoteParticipant, emit func(domain.Frame)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closing {
		return
	}

	if r.active.Add(1) > int64(r.source.cfg.MaxTracks) {
		r.active.Add(-1)

		select {
		case r.failure <- media.ErrLimit:
		default:
		}

		return
	}

	codec := track.Codec()
	info := domain.Track{
		Handle:      r.handles.Add(1),
		ID:          pub.SID(),
		Participant: participant.Identity(),
		Kind:        kind(pub),

		MIME:      codec.MimeType,
		ClockRate: codec.ClockRate,
		Channels:  uint32(codec.Channels),
		FMTP:      codec.SDPFmtpLine,
	}

	r.wg.Go(func() {
		defer r.active.Add(-1)
		defer func() {
			emit(domain.Frame{
				Track:      &info,
				ReceivedAt: time.Now(),
				Ended:      true,
			})
		}()

		if readErr := r.source.read(ctx, track, &info, emit); readErr != nil {
			select {
			case r.failure <- readErr:
			default:
			}
		}
	})
}

func selectTracks(room *lksdk.Room, wants func(domain.Track) bool, requested map[string]bool) error {
	seen := make(map[string]struct{})

	for _, participant := range room.GetRemoteParticipants() {
		for _, publication := range participant.TrackPublications() {
			pub, ok := publication.(*lksdk.RemoteTrackPublication)
			if !ok {
				continue
			}

			seen[pub.SID()] = struct{}{}

			selected := wants(domain.Track{
				ID:          pub.SID(),
				Participant: participant.Identity(),
				Kind:        kind(pub),
			})
			if selected == requested[pub.SID()] {
				continue
			}

			if err := pub.SetSubscribed(selected); err != nil {
				return fmt.Errorf("select track: %w: %w", media.ErrSource, err)
			}

			requested[pub.SID()] = selected
		}
	}

	for id := range requested {
		if _, ok := seen[id]; !ok {
			delete(requested, id)
		}
	}

	return nil
}

func (s *Source) read(ctx context.Context, track *webrtc.TrackRemote, info *domain.Track, emit func(domain.Frame)) error {
	buffer := make([]byte, s.cfg.MaxPacketBytes)
	for ctx.Err() == nil {
		if err := track.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout)); err != nil {
			return fmt.Errorf("set media read deadline: %w: %w", media.ErrSource, err)
		}

		n, _, err := track.Read(buffer)
		if err != nil {
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				continue
			}

			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return ctx.Err()
			}

			return fmt.Errorf("read track: %w: %w", media.ErrSource, err)
		}

		if n < rtpHeaderBytes || n > s.cfg.MaxPacketBytes {
			return media.ErrSource
		}

		emit(domain.Frame{
			Track:      info,
			Data:       append([]byte(nil), buffer[:n]...),
			ReceivedAt: time.Now(),
		})
	}

	return nil
}

func kind(pub lksdk.TrackPublication) domain.MediaKind {
	if pub.Kind() == lksdk.TrackKindAudio {
		return domain.Audio
	}

	return domain.Video
}
