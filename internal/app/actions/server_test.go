package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livekit/protocol/livekit"
	"github.com/twitchtv/twirp"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/dwspurpleteam/streamapi/internal/pkg/gen/streamapi/v1"
)

type fakeRooms struct {
	muted   []*livekit.MuteRoomTrackRequest
	sent    []*livekit.SendDataRequest
	missing bool
	removed []*livekit.RoomParticipantIdentity
	updated []*livekit.UpdateParticipantRequest
}

const (
	testCall          = "RM_call"
	testActive        = "ACTIVE"
	testEventName     = "subtitle.updated"
	testParticipant   = "alice"
	testAuthorization = "Bearer secret"
	testActivation    = "11111111-1111-4111-8111-111111111111"
	testRoom          = "room"
)

func TestGRPCMuteMicrophone(t *testing.T) {
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"callId":"RM_call","state":"ACTIVE"}`)
	}))
	defer manager.Close()

	room := &fakeRooms{}
	listener := bufconn.Listen(1024 * 1024)
	rpc := grpc.NewServer()

	pb.RegisterActionServiceServer(rpc, New(manager.URL, "secret", manager.Client(), room))
	go func() {
		if err := rpc.Serve(listener); err != nil {
			t.Errorf("grpc server: %v", err)
		}
	}()

	defer rpc.Stop()

	conn, err := grpc.NewClient("passthrough:///streamapi", grpc.WithContextDialer(
		func(context.Context, string) (net.Conn, error) { return listener.Dial() },
	), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Errorf("close grpc connection: %v", closeErr)
		}
	}()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", testAuthorization)

	_, err = pb.NewActionServiceClient(conn).MuteMicrophone(ctx, &pb.MuteMicrophoneRequest{
		ActivationId: testActivation, CallId: testCall, ParticipantId: testParticipant,
	})
	if err != nil || len(room.muted) != 1 {
		t.Fatalf("mute failed: %v, tracks: %v", err, room.muted)
	}
}

func (f *fakeRooms) ListRooms(context.Context, *livekit.ListRoomsRequest) (*livekit.ListRoomsResponse, error) {
	return &livekit.ListRoomsResponse{Rooms: []*livekit.Room{{Sid: testCall, Name: testRoom}}}, nil
}

func (f *fakeRooms) GetParticipant(_ context.Context, req *livekit.RoomParticipantIdentity) (*livekit.ParticipantInfo, error) {
	if f.missing || req.GetRoom() != testRoom || req.GetIdentity() != testParticipant {
		return nil, twirp.NotFoundError("participant not found")
	}

	return &livekit.ParticipantInfo{
		Identity:   testParticipant,
		Permission: &livekit.ParticipantPermission{CanPublish: true, CanSubscribe: true, CanPublishData: true, Hidden: true},
		Tracks: []*livekit.TrackInfo{
			{Sid: "TR_mic", Source: livekit.TrackSource_MICROPHONE},
			{Sid: "TR_screen", Source: livekit.TrackSource_SCREEN_SHARE_AUDIO},
		},
	}, nil
}

func (f *fakeRooms) RemoveParticipant(_ context.Context, req *livekit.RoomParticipantIdentity) (*livekit.RemoveParticipantResponse, error) {
	f.removed = append(f.removed, req)

	return &livekit.RemoveParticipantResponse{}, nil
}

func (f *fakeRooms) UpdateParticipant(_ context.Context, req *livekit.UpdateParticipantRequest) (*livekit.ParticipantInfo, error) {
	f.updated = append(f.updated, req)

	return &livekit.ParticipantInfo{}, nil
}

func TestParticipantActions(t *testing.T) {
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"callId":"RM_call","state":"ACTIVE"}`)
	}))
	defer manager.Close()

	room := &fakeRooms{}
	server := New(manager.URL, "secret", manager.Client(), room)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", testAuthorization))

	req := &pb.SendEventRequest{
		CallId: testCall, ActivationId: testActivation,
		ParticipantIds: []string{testParticipant}, EventName: updatePermissionsEvent, Data: `{"canPublish":false}`,
	}
	if _, err := server.SendEvent(ctx, req); err != nil {
		t.Fatal(err)
	}

	if len(room.updated) != 1 || room.updated[0].GetPermission().GetCanPublish() ||
		!room.updated[0].GetPermission().GetCanSubscribe() || !room.updated[0].GetPermission().GetCanPublishData() ||
		!room.updated[0].GetPermission().GetHidden() {
		t.Fatalf("permissions not preserved: %v", room.updated)
	}

	for _, data := range []string{`{}`, `{"canPublish":"false"}`, `{"other":true}`, `{"canPublish":false} {}`} {
		req.Data = data
		if _, err := server.SendEvent(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid permissions %q: %v", data, err)
		}
	}

	req.Data = ""
	req.EventName = removeParticipantEvent

	req.ParticipantIds = []string{"outside"}
	if _, err := server.SendEvent(ctx, req); status.Code(err) != codes.NotFound {
		t.Fatalf("outside participant: %v", err)
	}

	if len(room.removed) != 0 {
		t.Fatal("removed participant outside call")
	}

	req.ParticipantIds = []string{testParticipant}
	if _, err := server.SendEvent(ctx, req); err != nil {
		t.Fatal(err)
	}

	if len(room.removed) != 1 || room.removed[0].GetRoom() != testRoom || room.removed[0].GetIdentity() != testParticipant {
		t.Fatalf("unexpected removal: %v", room.removed)
	}
}

func (f *fakeRooms) MutePublishedTrack(_ context.Context, req *livekit.MuteRoomTrackRequest) (*livekit.MuteRoomTrackResponse, error) {
	f.muted = append(f.muted, req)

	return &livekit.MuteRoomTrackResponse{}, nil
}

func (f *fakeRooms) SendData(_ context.Context, req *livekit.SendDataRequest) (*livekit.SendDataResponse, error) {
	f.sent = append(f.sent, req)

	return &livekit.SendDataResponse{}, nil
}

func TestSendEvent(t *testing.T) {
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"callId":"RM_call","state":"ACTIVE"}`)
	}))
	defer manager.Close()

	room := &fakeRooms{}
	server := New(manager.URL, "secret", manager.Client(), room)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", testAuthorization))

	request := &pb.SendEventRequest{
		CallId: testCall, ActivationId: testActivation,
		EventName: testEventName, ParticipantIds: []string{testParticipant}, Data: "hello",
	}
	if _, err := server.SendEvent(ctx, request); err != nil {
		t.Fatal(err)
	}

	if len(room.sent) != 1 || room.sent[0].GetTopic() != pluginEventTopic || room.sent[0].GetKind() != livekit.DataPacket_RELIABLE {
		t.Fatalf("unexpected sent events: %v", room.sent)
	}

	var envelope struct {
		ActivationID string `json:"activationId"`
		CallID       string `json:"callId"`
		Name         string `json:"name"`
		Data         string `json:"data"`
	}
	if err := json.Unmarshal(room.sent[0].GetData(), &envelope); err != nil {
		t.Fatal(err)
	}

	if envelope.ActivationID != request.GetActivationId() || envelope.CallID != testCall ||
		envelope.Name != testEventName || envelope.Data != "hello" ||
		len(room.sent[0].GetDestinationIdentities()) != 1 || room.sent[0].GetDestinationIdentities()[0] != testParticipant {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}

	request.EventName = muteMicrophoneEvent

	request.Data = ""
	if _, err := server.SendEvent(ctx, request); err != nil {
		t.Fatal(err)
	}

	if len(room.muted) != 1 || len(room.sent) != 1 {
		t.Fatal("microphone event must mute, not send data")
	}

	request.ParticipantIds = nil
	if _, err := server.SendEvent(ctx, request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty recipients: %v", err)
	}

	request.EventName = testEventName

	request.ParticipantIds = []string{"other"}
	if _, err := server.SendEvent(ctx, request); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown recipient: %v", err)
	}

	if len(room.sent) != 1 {
		t.Fatal("event was sent to an unknown recipient")
	}
}

func TestMuteMicrophone(t *testing.T) {
	const activation = testActivation

	cases := []struct {
		name    string
		token   string
		call    string
		state   string
		missing bool
		want    codes.Code
	}{
		{name: "active", token: testAuthorization, call: testCall, state: testActive, want: codes.OK},
		{name: "wrong token", token: "Bearer wrong", call: testCall, state: testActive, want: codes.Unauthenticated},
		{name: "inactive", token: testAuthorization, call: testCall, state: "STOPPED", want: codes.FailedPrecondition},
		{name: "wrong call", token: testAuthorization, call: "RM_other", state: testActive, want: codes.FailedPrecondition},
		{name: "participant outside call", token: testAuthorization, call: testCall, state: testActive, missing: true, want: codes.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/activations/"+activation {
					t.Errorf("unexpected path %s", r.URL.Path)
				}

				_, _ = fmt.Fprintf(w, `{"callId":"RM_call","state":%q}`, tc.state)
			}))
			defer manager.Close()

			room := &fakeRooms{missing: tc.missing}
			server := New(manager.URL, "secret", manager.Client(), room)
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", tc.token))

			_, err := server.MuteMicrophone(ctx, &pb.MuteMicrophoneRequest{ActivationId: activation, CallId: tc.call, ParticipantId: testParticipant})
			if got := status.Code(err); got != tc.want {
				t.Fatalf("code = %v, want %v: %v", got, tc.want, err)
			}

			if tc.want == codes.OK {
				if len(room.muted) != 1 || room.muted[0].GetTrackSid() != "TR_mic" || !room.muted[0].GetMuted() {
					t.Fatalf("unexpected muted tracks: %v", room.muted)
				}
			} else if len(room.muted) != 0 {
				t.Fatal("rejected action reached livekit")
			}
		})
	}
}
