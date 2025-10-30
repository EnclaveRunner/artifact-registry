package main

import (
	"artifact-registry/config"
	"artifact-registry/procedures"
	proto "artifact-registry/proto_gen"

	"github.com/EnclaveRunner/shareddeps"
	"github.com/spf13/viper"
)

func main() {
	//nolint:mnd // Default port for gRPC service
	viper.SetDefault("port", 9876)
	viper.SetDefault("log_level", "info")
	viper.SetDefault("human_readable_output", "true")
	// initialize gRPC server
	shareddeps.InitGRPCServer(config.Cfg, "artifact-registry", "v0.0.0")

	proto.RegisterRegistryServiceServer(
		shareddeps.GRPCServer,
		&procedures.Server{},
	)

	shareddeps.StartGRPCServer()
}
