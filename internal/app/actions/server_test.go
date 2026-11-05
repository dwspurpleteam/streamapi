package actions

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/livekit/protocol/livekit"
	"github.com/twitchtv/twirp"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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
	testCall        = "RM_call"
	testEventName   = "subtitle.updated"
	testParticipant = "alice"
	testActivation  = "11111111-1111-4111-8111-111111111111"
	testRoom        = "room"
)

func TestGRPCMuteMicrophone(t *testing.T) {
	room := &fakeRooms{}
	listener := bufconn.Listen(1024 * 1024)
	rpc := grpc.NewServer()

	pb.RegisterActionServiceServer(rpc, New(room))
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

	ctx := context.Background()

	_, err = pb.NewActionServiceClient(conn).SendEvent(ctx, &pb.SendEventRequest{
		ActivationId: testActivation, CallId: testCall, ParticipantIds: []string{testParticipant}, EventName: muteMicrophoneEvent,
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
	room := &fakeRooms{}
	server := New(room)
	ctx := context.Background()

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
	room := &fakeRooms{}
	server := New(room)
	ctx := context.Background()

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
	cases := []struct {
		name, call string
		missing    bool
		want       codes.Code
	}{
		{name: "participant in call", call: testCall, want: codes.OK},
		{name: "unknown call", call: "RM_other", want: codes.NotFound},
		{name: "participant outside call", call: testCall, missing: true, want: codes.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			room := &fakeRooms{missing: tc.missing}
			server := New(room)

			_, err := server.SendEvent(context.Background(), &pb.SendEventRequest{
				ActivationId: testActivation, CallId: tc.call, ParticipantIds: []string{testParticipant}, EventName: muteMicrophoneEvent,
			})
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
