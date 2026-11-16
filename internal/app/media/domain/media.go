package domain

import "time"

const MaxIdentifierBytes = 256

type MediaKind uint8

const (
	Audio MediaKind = 1
	Video MediaKind = 2
)

const (
	Microphone uint8 = iota + 1
	Camera
	ScreenShare
	ScreenShareAudio
)

type Call struct {
	ID   string
	Room string
}

type Selection struct {
	Participant string
	Track       string
	Kinds       uint8
	Sources     uint8
}

func (s Selection) Matches(t Track) bool {
	return (s.Participant == "" || s.Participant == t.Participant) &&
		(s.Track == "" || s.Track == t.ID) &&
		(s.Kinds == 0 || s.Kinds&(1<<t.Kind) != 0) &&
		(s.Sources == 0 || s.Sources&(1<<t.Source) != 0)
}

type Track struct {
	Handle      uint32
	ID          string
	Participant string
	Kind        MediaKind
	Source      uint8
	MIME        string
	ClockRate   uint32
	Channels    uint32
	FMTP        string
}

type Frame struct {
	Track      *Track
	Data       []byte
	ReceivedAt time.Time
	Ended      bool
}
