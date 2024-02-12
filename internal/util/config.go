package util

import "github.com/spf13/viper"

type KafkaConfiguration struct {
	Brokers            string `mapstructure:"kafka_brokers"`
	Topic              string `mapstructure:"kafka_topic"`
	GroupID            string `mapstructure:"kafka_group_id"`
	AutoOffsetReset    string `mapstructure:"kafka_auto_offset_reset"`
	EnableAutoCommit   bool   `mapstructure:"kafka_enable_auto_commit"`
	AutoCommitInterval int    `mapstructure:"kafka_auto_commit_interval"`
	SessionTimeout     int    `mapstructure:"kafka_session_timeout"`
	HeartbeatInterval  int    `mapstructure:"kafka_heartbeat_interval"`
	MaxPollInterval    int    `mapstructure:"kafka_max_poll_interval"`
}

type Configuration struct {
	KafkaConfiguration *KafkaConfiguration
}

func NewConfiguration() *Configuration {
	viper.AddConfigPath(".")
	viper.SetConfigFile(".env")
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	var kafkaConfig KafkaConfiguration
	err = viper.Unmarshal(&kafkaConfig)

	if err != nil {
		panic(err)
	}

	return &Configuration{
		KafkaConfiguration: &kafkaConfig,
	}
}
