package configs

type Config struct {
	DurationSeconds int `mapstructure:"duration_seconds"`
	Throughput      int `mapstructure:"throughput_per_second"`
	NumClients      int `mapstructure:"num_clients"`

	Scenario        string           `mapstructure:"scenario"`
	PeriodicFailure *PeriodicFailure `mapstructure:"periodic_failure"`
}

type PeriodicFailure struct {
	IntervalSeconds int `mapstructure:"interval_seconds"`
}
