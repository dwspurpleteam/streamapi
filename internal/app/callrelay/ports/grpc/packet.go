package grpc

import (
	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/domain"
	pb "github.com/dwspurpleteam/callrelay/internal/pkg/gen/callrelay/v1"
)

type packetMessage struct {
	response pb.SubscribeResponse
	payload  pb.SubscribeResponse_Packet
	packet   pb.RtpPacket
}

func packetResponse(frame domain.Frame) *pb.SubscribeResponse {
	message := &packetMessage{}
	message.packet.Handle = frame.Track.Handle
	message.packet.Rtp = frame.Data
	message.packet.ReceivedAtUnixNano = frame.ReceivedAt.UnixNano()

	message.payload.Packet = &message.packet
	message.response.Payload = &message.payload

	return &message.response
}
