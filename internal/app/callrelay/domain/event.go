package domain

import "time"

const (
	MaxEventTypeBytes   = 128
	MaxEventSourceBytes = 64
)

type Event struct {
	ID          string
	Type        string
	Source      string
	CallID      string
	Room        string
	Participant string
	Track       string
	OccurredAt  time.Time
}

func ValidEventType(value string) bool {
	if value == "" || len(value) > MaxEventTypeBytes {
		return false
	}

	start := true
	for _, c := range value {
		if c == '.' && !start {
			start = true

			continue
		}

		if c < 'a' || c > 'z' {
			if start || (c < '0' || c > '9') && c != '_' && c != '-' {
				return false
			}
		}

		start = false
	}

	return !start
}
