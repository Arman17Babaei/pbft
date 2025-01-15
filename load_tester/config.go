package load_tester

type Config struct {
	DurationSeconds int `mapstructure:"duration_seconds"`
	Throughput      int `mapstructure:"throughput_per_second"`
	NumClients      int `mapstructure:"num_clients"`
}
