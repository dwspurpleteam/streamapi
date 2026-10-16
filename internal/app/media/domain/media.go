package domain

import "time"

const MaxIdentifierBytes = 256

type MediaKind uint8

const (
	Audio MediaKind = 1
	Video MediaKind = 2
)

type Call struct {
	ID   string
	Room string
}

type Selection struct {
	Participant string
	Track       string
	Kinds       uint8
}

func (s Selection) Matches(t Track) bool {
	return (s.Participant == "" || s.Participant == t.Participant) &&
		(s.Track == "" || s.Track == t.ID) &&
		(s.Kinds == 0 || s.Kinds&(1<<t.Kind) != 0)
}

type Track struct {
	Handle      uint32
	ID          string
	Participant string
	Kind        MediaKind
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
