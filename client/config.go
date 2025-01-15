package client

type Config struct {
	ClientId      string              `mapstructure:"client_id"`
	GrpcAddress   *Address            `mapstructure:"grpc_address"`
	HttpAddress   *Address            `mapstructure:"http_address"`
	NodesAddress  map[string]*Address `mapstructure:"nodes_address"`
	GrpcTimeoutMs int                 `mapstructure:"grpc_timeout_ms"`
}

type Address struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

func (c *Config) F() int {
	return (len(c.NodesAddress) - 1) / 3
}
