package permissions

import (
	"context"
	"strings"
	"time"

	routes "github.com/dwspurpleteam/streamapi/internal/pkg/gen/routes/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Client struct {
	routes  routes.RoutesServiceClient
	timeout time.Duration
}

func New(client routes.RoutesServiceClient, timeout time.Duration) *Client {
	return &Client{routes: client, timeout: timeout}
}

func credentials(ctx context.Context, activation string) (string, error) {
	if activation == "" {
		return "", status.Error(codes.InvalidArgument, "activation_id is required")
	}

	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || len(values[0]) <= len("Bearer ") {
		return "", status.Error(codes.Unauthenticated, "activation credential is required")
	}

	return strings.TrimPrefix(values[0], "Bearer "), nil
}

func (c *Client) Action(ctx context.Context, call, activation, action string) error {
	secret, err := credentials(ctx, activation)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.routes.CheckActionPermission(ctx, &routes.CheckActionPermissionRequest{CallId: call, ActivationId: activation, ActivationSecret: secret, Action: action})
	if err != nil {
		return status.Error(codes.Unavailable, "permission service unavailable")
	}

	if !result.GetAllowed() {
		return status.Error(codes.PermissionDenied, "action is not allowed")
	}

	return nil
}

func (c *Client) Media(ctx context.Context, call, activation string) (uint8, error) {
	secret, err := credentials(ctx, activation)
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var mask uint8

	for _, kind := range []routes.MediaType{
		routes.MediaType_MEDIA_TYPE_AUDIO,
		routes.MediaType_MEDIA_TYPE_VIDEO,
		routes.MediaType_MEDIA_TYPE_SCREEN_SHARE,
		routes.MediaType_MEDIA_TYPE_SCREEN_SHARE_AUDIO,
	} {
		result, err := c.routes.CheckMediaPermission(ctx, &routes.CheckMediaPermissionRequest{CallId: call, ActivationId: activation, ActivationSecret: secret, MediaType: kind})
		if err != nil {
			return 0, status.Error(codes.Unavailable, "permission service unavailable")
		}

		if result.GetCanSend() {
			mask |= 1 << kind
		}
	}

	if mask == 0 {
		return 0, status.Error(codes.PermissionDenied, "media is not allowed")
	}

	return mask, nil
}

var eventTypes = map[string]routes.EventType{
	"call.started":                      routes.EventType_EVENT_TYPE_CALL_STARTED,
	"call.ended":                        routes.EventType_EVENT_TYPE_CALL_ENDED,
	"participant.joined":                routes.EventType_EVENT_TYPE_PARTICIPANT_JOINED,
	"participant.left":                  routes.EventType_EVENT_TYPE_PARTICIPANT_LEFT,
	"participant.connection_aborted":    routes.EventType_EVENT_TYPE_PARTICIPANT_CONNECTION_ABORTED,
	"participant.reconnecting":          routes.EventType_EVENT_TYPE_PARTICIPANT_RECONNECTING,
	"participant.reconnected":           routes.EventType_EVENT_TYPE_PARTICIPANT_RECONNECTED,
	"participant.kicked":                routes.EventType_EVENT_TYPE_PARTICIPANT_KICKED,
	"media.audio_muted":                 routes.EventType_EVENT_TYPE_MEDIA_AUDIO_MUTED,
	"media.audio_unmuted":               routes.EventType_EVENT_TYPE_MEDIA_AUDIO_UNMUTED,
	"media.video_enabled":               routes.EventType_EVENT_TYPE_MEDIA_VIDEO_ENABLED,
	"media.video_disabled":              routes.EventType_EVENT_TYPE_MEDIA_VIDEO_DISABLED,
	"media.screen_share_started":        routes.EventType_EVENT_TYPE_MEDIA_SCREEN_SHARE_STARTED,
	"media.screen_share_stopped":        routes.EventType_EVENT_TYPE_MEDIA_SCREEN_SHARE_STOPPED,
	"media.track_published":             routes.EventType_EVENT_TYPE_MEDIA_TRACK_PUBLISHED,
	"media.track_unpublished":           routes.EventType_EVENT_TYPE_MEDIA_TRACK_UNPUBLISHED,
	"track.published":                   routes.EventType_EVENT_TYPE_MEDIA_TRACK_PUBLISHED,
	"track.unpublished":                 routes.EventType_EVENT_TYPE_MEDIA_TRACK_UNPUBLISHED,
	"interaction.chat_message_received": routes.EventType_EVENT_TYPE_INTERACTION_CHAT_MESSAGE_RECEIVED,
}

func (c *Client) Events(ctx context.Context, call, activation string, requested, available []string) ([]string, error) {
	secret, err := credentials(ctx, activation)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	explicit := len(requested) != 0
	if !explicit {
		requested = available
	}

	var allowed []string

	for _, name := range requested {
		kind, ok := eventTypes[name]
		if !ok {
			if explicit {
				return nil, status.Error(codes.PermissionDenied, "event is not allowed")
			}

			continue
		}

		result, err := c.routes.CheckEventPermission(ctx, &routes.CheckEventPermissionRequest{CallId: call, ActivationId: activation, ActivationSecret: secret, EventType: kind})
		if err != nil {
			return nil, status.Error(codes.Unavailable, "permission service unavailable")
		}

		if result.GetCanSend() {
			allowed = append(allowed, name)
		} else if explicit {
			return nil, status.Error(codes.PermissionDenied, "event is not allowed")
		}
	}

	if len(allowed) == 0 {
		return nil, status.Error(codes.PermissionDenied, "events are not allowed")
	}

	return allowed, nil
}
