package config

import (
	enclaveConfig "github.com/EnclaveRunner/shareddeps/config"
)

type AppConfig struct {
	enclaveConfig.BaseConfig `mapstructure:",squash"`

	StorageDir string `mapstructure:"storage_dir" validate:"omitempty"`

	Database struct {
		Host     string `mapstructure:"host"     validate:"required,hostname|ip"`
		Port     int    `mapstructure:"port"     validate:"required,numeric,min=1,max=65535"`
		Username string `mapstructure:"username" validate:"required"`
		Password string `mapstructure:"password" validate:"required"`
		Database string `mapstructure:"database" validate:"required"`
		SSLMode  string `mapstructure:"sslmode"  validate:"oneof=disable require verify-ca verify-full"`
	} `mapstructure:"database" validate:"required"`
}

//nolint:mnd // Default port for gRPC service
var Defaults = []enclaveConfig.DefaultValue{
	{Key: "port", Value: 9876},
	{Key: "log_level", Value: "info"},
	{Key: "human_readable_output", Value: "true"},
	{Key: "storage_dir", Value: "/data"},

	{Key: "database.port", Value: 5432},
	{Key: "database.host", Value: "localhost"},
	{Key: "database.sslmode", Value: "disable"},
	{Key: "database.username", Value: "enclave_user"},
	{Key: "database.password", Value: "enclave_password"},
	{Key: "database.database", Value: "enclave_db"},
}

var Cfg = &AppConfig{}
