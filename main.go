package main

import (
	"artifact-registry/config"
	"artifact-registry/filesystemRegistry"
	"artifact-registry/procedures"
	proto "artifact-registry/proto_gen"

	"github.com/EnclaveRunner/shareddeps"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func main() {
	//nolint:mnd // Default port for gRPC service
	viper.SetDefault("port", 9876)
	viper.SetDefault("log_level", "info")
	viper.SetDefault("human_readable_output", "true")

	// Initialize filesystem registry
	storageDir := filesystemRegistry.GetStorageDir()
	registry, err := filesystemRegistry.New(storageDir)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize filesystem registry")
	}

	log.Info().Str("storage_dir", storageDir).Msg("Filesystem registry initialized")

	// initialize gRPC server
	shareddeps.InitGRPCServer(config.Cfg, "artifact-registry", "v0.0.0")

	proto.RegisterRegistryServiceServer(
		shareddeps.GRPCServer,
		procedures.NewServer(registry),
	)

	shareddeps.StartGRPCServer()
}
