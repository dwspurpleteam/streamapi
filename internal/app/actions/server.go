package actions

import (
	"context"
	"crypto/subtle"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/dwspurpleteam/streamapi/internal/pkg/gen/streamapi/v1"
)

const (
	actionTimeout      = 10 * time.Second
	maxManagerResponse = 64 * 1024
)

type rooms interface {
	ListRooms(ctx context.Context, req *livekit.ListRoomsRequest) (*livekit.ListRoomsResponse, error)
	GetParticipant(ctx context.Context, req *livekit.RoomParticipantIdentity) (*livekit.ParticipantInfo, error)
	MutePublishedTrack(ctx context.Context, req *livekit.MuteRoomTrackRequest) (*livekit.MuteRoomTrackResponse, error)
}

type Server struct {
	pb.UnimplementedActionServiceServer

	managerURL string
	token      string
	client     *http.Client
	rooms      rooms
}

func New(managerURL, token string, client *http.Client, rooms rooms) *Server {
	return &Server{managerURL: strings.TrimRight(managerURL, "/"), token: token, client: client, rooms: rooms}
}

func (s *Server) MuteMicrophone(ctx context.Context, req *pb.MuteMicrophoneRequest) (*pb.MuteMicrophoneResponse, error) {
	if s.managerURL == "" || s.token == "" {
		return nil, status.Error(codes.Unavailable, "plugin actions are not configured")
	}

	md, _ := metadata.FromIncomingContext(ctx)

	auth := md.Get("authorization")
	if len(auth) != 1 || subtle.ConstantTimeCompare([]byte(auth[0]), []byte("Bearer "+s.token)) != 1 {
		return nil, status.Error(codes.Unauthenticated, "invalid action token")
	}

	id, err := uuid.Parse(req.GetActivationId())
	if err != nil || !validID(req.GetCallId()) || !validID(req.GetParticipantId()) {
		return nil, status.Error(codes.InvalidArgument, "invalid activation, call or participant")
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

	list, err := s.rooms.ListRooms(ctx, &livekit.ListRoomsRequest{})
	if err != nil {
		return nil, livekitError(ctx, err)
	}

	roomName := ""

	for _, room := range list.GetRooms() {
		if room.GetSid() == callID {
			roomName = room.GetName()

			break
		}
	}

	if roomName == "" {
		return nil, status.Error(codes.NotFound, "call not found")
	}

	participant, err := s.rooms.GetParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room: roomName, Identity: req.GetParticipantId(),
	})
	if err != nil {
		return nil, livekitError(ctx, err)
	}

	muted := false

	for _, track := range participant.GetTracks() {
		if track.GetSource() != livekit.TrackSource_MICROPHONE {
			continue
		}

		_, err = s.rooms.MutePublishedTrack(ctx, &livekit.MuteRoomTrackRequest{
			Room: roomName, Identity: participant.GetIdentity(), TrackSid: track.GetSid(), Muted: true,
		})
		if err != nil {
			return nil, livekitError(ctx, err)
		}

		muted = true
	}

	if !muted {
		return nil, status.Error(codes.NotFound, "microphone not found")
	}

	return &pb.MuteMicrophoneResponse{}, nil
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
