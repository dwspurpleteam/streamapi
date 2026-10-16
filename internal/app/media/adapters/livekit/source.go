package livekit

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"time"

	"github.com/livekit/protocol/auth"
	lk "github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"

	"github.com/dwspurpleteam/streamapi/internal/app/media/domain"
	media "github.com/dwspurpleteam/streamapi/internal/app/media/service"
)

type Config struct {
	Name           string
	URL            string
	APIKey         string
	APISecret      string
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	TokenTTL       time.Duration
	MaxTracks      int
	MaxPacketBytes int
}

type Source struct {
	cfg    Config
	client *lksdk.RoomServiceClient
	logger *slog.Logger
}

func New(cfg Config, logger *slog.Logger) *Source {
	return &Source{
		cfg:    cfg,
		client: lksdk.NewRoomServiceClient(cfg.URL, cfg.APIKey, cfg.APISecret),
		logger: logger,
	}
}

func (s *Source) Run(
	ctx context.Context,
	call domain.Call,
	emit func(domain.Frame),
	wants func(domain.Track) bool,
	changed <-chan struct{},
) error {
	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	joinCtx, cancelJoin := context.WithTimeout(ctx, s.cfg.ConnectTimeout)
	defer cancelJoin()

	token, err := s.token(joinCtx, call)
	if err != nil {
		return err
	}

	refresh := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	failure := make(chan error, 1)

	notify := func() {
		select {
		case refresh <- struct{}{}:
		default:
		}
	}

	readers := trackReaders{
		source:  s,
		failure: failure,
	}

	cb := lksdk.NewRoomCallback()
	cb.OnDisconnected = func() {
		select {
		case stopped <- struct{}{}:
		default:
		}
	}

	cb.OnReconnected = notify
	cb.OnTrackPublished = func(*lksdk.RemoteTrackPublication, *lksdk.RemoteParticipant) {
		notify()
	}
	cb.OnTrackUnpublished = func(*lksdk.RemoteTrackPublication, *lksdk.RemoteParticipant) {
		notify()
	}
	cb.OnParticipantConnected = func(*lksdk.RemoteParticipant) {
		notify()
	}

	cb.OnTrackSubscriptionFailed = func(string, *lksdk.RemoteParticipant) {
		select {
		case failure <- media.ErrSource:
		default:
		}
	}

	cb.OnTrackSubscribed = func(
		track *webrtc.TrackRemote,
		pub *lksdk.RemoteTrackPublication,
		participant *lksdk.RemoteParticipant,
	) {
		readers.subscribe(
			ctx,
			track,
			pub,
			participant,
			emit,
		)
	}

	room := lksdk.NewRoom(cb)

	defer func() {
		cancelRun()

		readers.mu.Lock()
		readers.closing = true
		readers.mu.Unlock()

		room.Disconnect()
		readers.wg.Wait()
	}()

	if err := room.JoinWithContextAndToken(
		joinCtx,
		s.cfg.URL,
		token,
		lksdk.WithAutoSubscribe(false),
	); err != nil {
		return fmt.Errorf("join call: %w: %w", media.ErrSource, err)
	}

	if room.SID() != call.ID {
		return media.ErrNotFound
	}

	s.logger.Info("media source connected", "call_id", call.ID)
	defer s.logger.Info("media source disconnected", "call_id", call.ID)

	notify()

	requested := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stopped:
			return media.ErrSource
		case err := <-failure:
			return err
		case <-changed:
		case <-refresh:
		}

		if room.SID() != call.ID {
			return media.ErrNotFound
		}

		if err := selectTracks(room, wants, requested); err != nil {
			return err
		}
	}
}

func (s *Source) token(ctx context.Context, call domain.Call) (string, error) {
	list, err := s.client.ListRooms(ctx, &lk.ListRoomsRequest{Names: []string{call.Room}})
	if err != nil {
		return "", fmt.Errorf("list call: %w: %w", media.ErrSource, err)
	}

	if len(list.GetRooms()) != 1 || list.GetRooms()[0].GetSid() != call.ID {
		return "", media.ErrNotFound
	}

	no := false

	token, err := auth.NewAccessToken(s.cfg.APIKey, s.cfg.APISecret).
		SetIdentity(s.cfg.Name + "-" + rand.Text()).
		SetKind(lk.ParticipantInfo_AGENT).
		SetValidFor(s.cfg.TokenTTL).
		SetVideoGrant(&auth.VideoGrant{
			RoomJoin:       true,
			Room:           call.Room,
			Hidden:         true,
			CanSubscribe:   new(true),
			CanPublish:     &no,
			CanPublishData: &no,
		}).
		ToJWT()
	if err != nil {
		return "", fmt.Errorf("create media token: %w: %w", media.ErrSource, err)
	}

	return token, nil
}
