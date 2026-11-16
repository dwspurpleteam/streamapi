package permissions

import (
	"context"
	"errors"
	"testing"
	"time"

	routes "github.com/dwspurpleteam/streamapi/internal/pkg/gen/routes/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeRoutes struct {
	actionAllowed bool
	failure       bool
	lastSecret    string
}

var errUnavailable = errors.New("unavailable")

func (f *fakeRoutes) CheckActionPermission(_ context.Context, req *routes.CheckActionPermissionRequest, _ ...grpc.CallOption) (*routes.CheckActionPermissionResponse, error) {
	f.lastSecret = req.GetActivationSecret()
	if f.failure {
		return nil, errUnavailable
	}

	return &routes.CheckActionPermissionResponse{Allowed: f.actionAllowed}, nil
}

func (f *fakeRoutes) CheckEventPermission(_ context.Context, req *routes.CheckEventPermissionRequest, _ ...grpc.CallOption) (*routes.CheckEventPermissionResponse, error) {
	f.lastSecret = req.GetActivationSecret()
	if f.failure {
		return nil, errUnavailable
	}

	return &routes.CheckEventPermissionResponse{CanSend: req.GetEventType() == routes.EventType_EVENT_TYPE_PARTICIPANT_JOINED}, nil
}

func (f *fakeRoutes) CheckMediaPermission(_ context.Context, req *routes.CheckMediaPermissionRequest, _ ...grpc.CallOption) (*routes.CheckMediaPermissionResponse, error) {
	f.lastSecret = req.GetActivationSecret()
	if f.failure {
		return nil, errUnavailable
	}

	return &routes.CheckMediaPermissionResponse{CanSend: req.GetMediaType() == routes.MediaType_MEDIA_TYPE_AUDIO}, nil
}

func TestChecksActivationPermissions(t *testing.T) {
	service := &fakeRoutes{actionAllowed: true}
	client := New(service, time.Second)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	if err := client.Action(ctx, "call", "activation", "microphone.mute"); err != nil || service.lastSecret != "secret" {
		t.Fatalf("action: %v", err)
	}

	kinds, err := client.Media(ctx, "call", "activation")
	if err != nil || kinds != 1<<routes.MediaType_MEDIA_TYPE_AUDIO {
		t.Fatalf("media: %d %v", kinds, err)
	}

	names, err := client.Events(ctx, "call", "activation", nil, []string{"participant.joined", "participant.left"})
	if err != nil || len(names) != 1 || names[0] != "participant.joined" {
		t.Fatalf("events: %v %v", names, err)
	}

	_, err = client.Events(ctx, "call", "activation", []string{"participant.left"}, nil)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unexpected error: %v", err)
	}

	service.actionAllowed = false

	if status.Code(client.Action(ctx, "call", "activation", "x")) != codes.PermissionDenied {
		t.Fatal("action allowed")
	}

	if status.Code(client.Action(context.Background(), "call", "activation", "x")) != codes.Unauthenticated {
		t.Fatal("missing credential accepted")
	}

	service.failure = true

	if status.Code(client.Action(ctx, "call", "activation", "x")) != codes.Unavailable {
		t.Fatal("service failure accepted")
	}
}
