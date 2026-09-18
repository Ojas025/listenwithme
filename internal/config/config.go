package config

type Config struct {
	Monitor   string `json:"monitor"`
	QueueSize int    `json:"queueSize"`
	ChunkSize int    `json:"chunkSize"`
	Port      string `json:"port"`
}

func LoadConfig() Config {
	return Config{
		Monitor:   "bluez_output.84_9D_4B_82_7E_4B.1.monitor",
		QueueSize: 64,
		ChunkSize: 4096,
		Port:      "8080",
	}
}
