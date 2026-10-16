package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	configfile "github.com/dwspurpleteam/callrelay/internal/pkg/config"
)

type config struct {
	GRPC            grpcConfig    `yaml:"grpc"`
	HTTP            httpConfig    `yaml:"http"`
	Media           mediaConfig   `yaml:"media"`
	Events          eventsConfig  `yaml:"events"`
	LiveKit         livekitConfig `yaml:"livekit"`
	NATS            natsConfig    `yaml:"nats"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type grpcConfig struct {
	LatencyBucketsSeconds []float64     `yaml:"latency_buckets_seconds"`
	MaxConcurrentStreams  uint32        `yaml:"max_concurrent_streams"`
	Address               string        `yaml:"address"`
	Connections           int           `yaml:"connections"`
	SendTimeout           time.Duration `yaml:"send_timeout"`
	MaxMessageBytes       int           `yaml:"max_message_bytes"`
	MaxHeaderBytes        uint32        `yaml:"max_header_bytes"`
	WriteBufferBytes      int           `yaml:"write_buffer_bytes"`
	StreamWindowBytes     int32         `yaml:"stream_window_bytes"`
	ConnectionWindowBytes int32         `yaml:"connection_window_bytes"`
	KeepaliveTime         time.Duration `yaml:"keepalive_time"`
	KeepaliveTimeout      time.Duration `yaml:"keepalive_timeout"`
}

type httpConfig struct {
	MetricsConcurrency int           `yaml:"metrics_concurrency"`
	MetricsTimeout     time.Duration `yaml:"metrics_timeout"`
	Address            string        `yaml:"address"`
	Connections        int           `yaml:"connections"`
	Timeout            time.Duration `yaml:"timeout"`
	MaxHeaderBytes     int           `yaml:"max_header_bytes"`
	MaxBodyBytes       int64         `yaml:"max_body_bytes"`
	Concurrency        int           `yaml:"concurrency"`
	Profiling          bool          `yaml:"profiling"`
}

type mediaConfig struct {
	MaxSubscriptions     int           `yaml:"max_subscriptions"`
	MaxPerCall           int           `yaml:"max_per_call"`
	QueuePackets         int           `yaml:"queue_packets"`
	QueueBytes           int           `yaml:"queue_bytes"`
	QueueAge             time.Duration `yaml:"queue_age"`
	MaxReservedBytes     int64         `yaml:"max_reserved_bytes"`
	EgressBytesPerSecond int64         `yaml:"egress_bytes_per_second"`
	EgressBurstBytes     int64         `yaml:"egress_burst_bytes"`
}

type eventsConfig struct {
	Concurrency    int           `yaml:"concurrency"`
	PublishTimeout time.Duration `yaml:"publish_timeout"`
}

type livekitConfig struct {
	URL            string            `yaml:"url"`
	ConnectTimeout time.Duration     `yaml:"connect_timeout"`
	ReadTimeout    time.Duration     `yaml:"read_timeout"`
	TokenTTL       time.Duration     `yaml:"token_ttl"`
	MaxTracks      int               `yaml:"max_tracks"`
	MaxPacketBytes int               `yaml:"max_packet_bytes"`
	EventSource    string            `yaml:"event_source"`
	EventTypes     map[string]string `yaml:"event_types"`
}

type natsConfig struct {
	URL              string        `yaml:"url"`
	Stream           string        `yaml:"stream"`
	Prefix           string        `yaml:"prefix"`
	MaxBytes         int64         `yaml:"max_bytes"`
	MaxMessageBytes  int32         `yaml:"max_message_bytes"`
	MaxAge           time.Duration `yaml:"max_age"`
	DuplicateWindow  time.Duration `yaml:"duplicate_window"`
	Timeout          time.Duration `yaml:"timeout"`
	ReconnectWait    time.Duration `yaml:"reconnect_wait"`
	RecoveryInterval time.Duration `yaml:"recovery_interval"`
}

type secrets struct {
	LiveKitKey    string
	LiveKitSecret string
	NATSUser      string
	NATSPassword  string
}

func loadConfig() (*config, secrets, error) {
	configuration := flag.String("config", "configs/local.yaml", "YAML configuration file")

	flag.Parse()

	var cfg config
	if err := configfile.Load(*configuration, &cfg); err != nil {
		return nil, secrets{}, fmt.Errorf("load configuration: %w", err)
	}

	return &cfg, loadSecrets(), nil
}

func loadSecrets() secrets {
	return secrets{
		LiveKitKey:    os.Getenv("LIVEKIT_API_KEY"),
		LiveKitSecret: os.Getenv("LIVEKIT_API_SECRET"),
		NATSUser:      os.Getenv("NATS_USER"),
		NATSPassword:  os.Getenv("NATS_PASSWORD"),
	}
}
