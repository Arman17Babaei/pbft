package configs

type Config struct {
	Id           string              `mapstructure:"id"`
	Address      *Address            `mapstructure:"address"`
	PeersAddress map[string]*Address `mapstructure:"peers_address"`
	Grpc         *Grpc               `mapstructure:"grpc"`
	Timers       *Timers             `mapstructure:"timers"`
	General      *General            `mapstructure:"general"`
}

type Address struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type Grpc struct {
	SendTimeoutMs        int `mapstructure:"send_timeout_ms"`
	MaxRetries           int `mapstructure:"max_retries"`
	MaxConcurrentStreams int `mapstructure:"max_concurrent_streams"`
}

type Timers struct {
	ViewChangeTimeoutMs int `mapstructure:"view_change_timeout_ms"`
}

type General struct {
	EnabledByDefault       bool `mapstructure:"enabled_by_default"`
	MaxOutstandingRequests int  `mapstructure:"max_outstanding_requests"`
	CheckpointInterval     int  `mapstructure:"checkpoint_interval"`
	WaterMarkInterval      int  `mapstructure:"water_mark_interval"`
}

func (c *Config) F() int {
	return (len(c.PeersAddress) - 1) / 3
}
