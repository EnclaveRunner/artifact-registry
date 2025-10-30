package config

import (
	enclaveConfig "github.com/EnclaveRunner/shareddeps/config"
)

type AppConfig struct {
	enclaveConfig.BaseConfig `mapstructure:",squash"`
	
	Port     int    `mapstructure:"port"     validate:"required,numeric,min=1,max=65535"`
}

var Cfg = &AppConfig{}