package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/livekit/protocol/livekit"
	"github.com/twitchtv/twirp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/dwspurpleteam/streamapi/internal/pkg/gen/streamapi/v1"
)

const (
	actionTimeout          = 10 * time.Second
	maxManagerResponse     = 64 * 1024
	maxEventBytes          = 15 * 1024
	maxRecipients          = 64
	pluginEventTopic       = "plugin.event.v1"
	muteMicrophoneEvent    = "microphone.mute"
	removeParticipantEvent = "participant.remove"
	updatePermissionsEvent = "participant.permissions.update"
)

//nolint:interfacebloat // The LiveKit operations needed to deliver events and actions.
type rooms interface {
	ListRooms(ctx context.Context, req *livekit.ListRoomsRequest) (*livekit.ListRoomsResponse, error)
	GetParticipant(ctx context.Context, req *livekit.RoomParticipantIdentity) (*livekit.ParticipantInfo, error)
	MutePublishedTrack(ctx context.Context, req *livekit.MuteRoomTrackRequest) (*livekit.MuteRoomTrackResponse, error)
	SendData(ctx context.Context, req *livekit.SendDataRequest) (*livekit.SendDataResponse, error)
	RemoveParticipant(ctx context.Context, req *livekit.RoomParticipantIdentity) (*livekit.RemoveParticipantResponse, error)
	UpdateParticipant(ctx context.Context, req *livekit.UpdateParticipantRequest) (*livekit.ParticipantInfo, error)
}

type Server struct {
	pb.UnimplementedActionServiceServer

	managerURL string
	client     *http.Client
	rooms      rooms
}

func New(managerURL string, client *http.Client, rooms rooms) *Server {
	return &Server{managerURL: strings.TrimRight(managerURL, "/"), client: client, rooms: rooms}
}

func (s *Server) SendEvent(ctx context.Context, req *pb.SendEventRequest) (*pb.SendEventResponse, error) {
	if s.managerURL == "" {
		return nil, status.Error(codes.Unavailable, "plugin actions are not configured")
	}

	id, err := validateEvent(req)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, actionTimeout)
	defer cancel()

	callID, err := s.activationCall(ctx, id.String())
	if err != nil {
		return nil, err
	}

	if callID != req.GetCallId() {
		return nil, status.Error(codes.FailedPrecondition, "activation does not belong to this call")
	}

	room, err := s.findRoom(ctx, callID)
	if err != nil {
		return nil, err
	}

	switch req.GetEventName() {
	case muteMicrophoneEvent:
		if err := s.muteMicrophone(ctx, room, req.GetParticipantIds()[0]); err != nil {
			return nil, err
		}
	case removeParticipantEvent:
		if err := s.removeParticipant(ctx, room, req.GetParticipantIds()[0]); err != nil {
			return nil, err
		}
	case updatePermissionsEvent:
		if err := s.updatePermissions(ctx, room, req.GetParticipantIds()[0], req.GetData()); err != nil {
			return nil, err
		}
	default:
		if err := s.sendPluginEvent(ctx, room, id.String(), req); err != nil {
			return nil, err
		}
	}

	return &pb.SendEventResponse{}, nil
}

func validateEvent(req *pb.SendEventRequest) (uuid.UUID, error) {
	id, err := uuid.Parse(req.GetActivationId())
	if err != nil || !validID(req.GetCallId()) || !validID(req.GetEventName()) || len(req.GetParticipantIds()) == 0 || len(req.GetParticipantIds()) > maxRecipients {
		return uuid.Nil, status.Error(codes.InvalidArgument, "invalid activation, call, event or recipients")
	}

	for _, participant := range req.GetParticipantIds() {
		if !validID(participant) {
			return uuid.Nil, status.Error(codes.InvalidArgument, "invalid participant")
		}
	}

	if req.GetEventName() == muteMicrophoneEvent && (len(req.GetParticipantIds()) != 1 || req.GetData() != "") {
		return uuid.Nil, status.Error(codes.InvalidArgument, "microphone.mute requires one participant and no data")
	}

	if req.GetEventName() == removeParticipantEvent && (len(req.GetParticipantIds()) != 1 || req.GetData() != "") {
		return uuid.Nil, status.Error(codes.InvalidArgument, "participant.remove requires one participant and no data")
	}

	if req.GetEventName() == updatePermissionsEvent && len(req.GetParticipantIds()) != 1 {
		return uuid.Nil, status.Error(codes.InvalidArgument, "participant.permissions.update requires one participant")
	}

	return id, nil
}

func (s *Server) removeParticipant(ctx context.Context, room, identity string) error {
	req := &livekit.RoomParticipantIdentity{Room: room, Identity: identity}
	if _, err := s.rooms.GetParticipant(ctx, req); err != nil {
		return livekitError(ctx, err)
	}

	if _, err := s.rooms.RemoveParticipant(ctx, req); err != nil {
		return livekitError(ctx, err)
	}

	return nil
}

func (s *Server) updatePermissions(ctx context.Context, room, identity, data string) error {
	var patch struct {
		CanPublish        *bool `json:"canPublish"`
		CanSubscribe      *bool `json:"canSubscribe"`
		CanPublishData    *bool `json:"canPublishData"`
		CanUpdateMetadata *bool `json:"canUpdateMetadata"`
	}

	decoder := json.NewDecoder(bytes.NewBufferString(data))
	decoder.DisallowUnknownFields()

	if len(data) > maxEventBytes || decoder.Decode(&patch) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		(patch.CanPublish == nil && patch.CanSubscribe == nil && patch.CanPublishData == nil && patch.CanUpdateMetadata == nil) {
		return status.Error(codes.InvalidArgument, "invalid participant permissions")
	}

	participant, err := s.rooms.GetParticipant(ctx, &livekit.RoomParticipantIdentity{Room: room, Identity: identity})
	if err != nil {
		return livekitError(ctx, err)
	}

	if participant.GetPermission() == nil {
		return status.Error(codes.FailedPrecondition, "participant permissions unavailable")
	}

	permissions := proto.CloneOf(participant.GetPermission())
	if patch.CanPublish != nil {
		permissions.CanPublish = *patch.CanPublish
	}

	if patch.CanSubscribe != nil {
		permissions.CanSubscribe = *patch.CanSubscribe
	}

	if patch.CanPublishData != nil {
		permissions.CanPublishData = *patch.CanPublishData
	}

	if patch.CanUpdateMetadata != nil {
		permissions.CanUpdateMetadata = *patch.CanUpdateMetadata
	}

	_, err = s.rooms.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{Room: room, Identity: identity, Permission: permissions})
	if err != nil {
		return livekitError(ctx, err)
	}

	return nil
}

func (s *Server) findRoom(ctx context.Context, callID string) (string, error) {
	list, err := s.rooms.ListRooms(ctx, &livekit.ListRoomsRequest{})
	if err != nil {
		return "", livekitError(ctx, err)
	}

	for _, room := range list.GetRooms() {
		if room.GetSid() == callID && room.GetName() != "" {
			return room.GetName(), nil
		}
	}

	return "", status.Error(codes.NotFound, "call not found")
}

func (s *Server) muteMicrophone(ctx context.Context, room, identity string) error {
	participant, err := s.rooms.GetParticipant(ctx, &livekit.RoomParticipantIdentity{Room: room, Identity: identity})
	if err != nil {
		return livekitError(ctx, err)
	}

	muted := false

	for _, track := range participant.GetTracks() {
		if track.GetSource() != livekit.TrackSource_MICROPHONE {
			continue
		}

		_, err = s.rooms.MutePublishedTrack(ctx, &livekit.MuteRoomTrackRequest{Room: room, Identity: participant.GetIdentity(), TrackSid: track.GetSid(), Muted: true})
		if err != nil {
			return livekitError(ctx, err)
		}

		muted = true
	}

	if !muted {
		return status.Error(codes.NotFound, "microphone not found")
	}

	return nil
}

func (s *Server) sendPluginEvent(ctx context.Context, room, activationID string, req *pb.SendEventRequest) error {
	payload, err := json.Marshal(struct {
		ActivationID string `json:"activationId"`
		CallID       string `json:"callId"`
		Name         string `json:"name"`
		Data         string `json:"data"`
	}{ActivationID: activationID, CallID: req.GetCallId(), Name: req.GetEventName(), Data: req.GetData()})
	if err != nil || len(payload) > maxEventBytes {
		return status.Error(codes.InvalidArgument, "event is too large")
	}

	for _, identity := range req.GetParticipantIds() {
		if _, participantErr := s.rooms.GetParticipant(ctx, &livekit.RoomParticipantIdentity{Room: room, Identity: identity}); participantErr != nil {
			return livekitError(ctx, participantErr)
		}
	}

	topic := pluginEventTopic

	_, err = s.rooms.SendData(ctx, &livekit.SendDataRequest{
		Room: room, Data: payload, Kind: livekit.DataPacket_RELIABLE,
		DestinationIdentities: req.GetParticipantIds(), Topic: &topic,
	})
	if err != nil {
		return livekitError(ctx, err)
	}

	return nil
}

func (s *Server) activationCall(ctx context.Context, id string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.managerURL+"/v1/activations/"+url.PathEscape(id), nil)
	if err != nil {
		return "", status.Error(codes.Unavailable, "plugin manager is not configured")
	}

	response, err := s.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "", status.FromContextError(ctx.Err()).Err()
		}

		return "", status.Error(codes.Unavailable, "plugin manager is unavailable")
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotFound {
		return "", status.Error(codes.NotFound, "activation not found")
	}

	if response.StatusCode != http.StatusOK {
		return "", status.Error(codes.Unavailable, "plugin manager is unavailable")
	}

	var activation struct {
		CallID string `json:"callId"`
		State  string `json:"state"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, maxManagerResponse)).Decode(&activation); err != nil {
		return "", status.Error(codes.Unavailable, "invalid plugin manager response")
	}

	if activation.State != "ACTIVE" {
		return "", status.Error(codes.FailedPrecondition, "activation is not active")
	}

	if !validID(activation.CallID) {
		return "", status.Error(codes.Unavailable, "invalid plugin manager response")
	}

	return activation.CallID, nil
}

func validID(id string) bool {
	return len(id) <= 256 && strings.TrimSpace(id) != "" && !strings.ContainsFunc(id, unicode.IsControl)
}

func livekitError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}

	var twirpErr twirp.Error
	if errors.As(err, &twirpErr) && twirpErr.Code() == twirp.NotFound {
		return status.Error(codes.NotFound, "participant or track not found")
	}

	return status.Error(codes.Unavailable, "livekit is unavailable")
}
