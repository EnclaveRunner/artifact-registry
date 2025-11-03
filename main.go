package main

import (
	"artifact-registry/config"
	"artifact-registry/filesystemRegistry"
	"artifact-registry/orm"
	"artifact-registry/procedures"
	proto "artifact-registry/proto_gen"

	"github.com/EnclaveRunner/shareddeps"
	"github.com/rs/zerolog/log"
)

func main() {
	// initialize gRPC server
	shareddeps.InitGRPCServer(config.Cfg, "artifact-registry", "v0.0.0", config.Defaults...)
	orm.InitDB()
	// Initialize filesystem registry
	storageDir := filesystemRegistry.GetStorageDir()
	registry, err := filesystemRegistry.New(storageDir)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize filesystem registry")
	}

	log.Info().Str("storage_dir", storageDir).Msg("Filesystem registry initialized")

	proto.RegisterRegistryServiceServer(
		shareddeps.GRPCServer,
		procedures.NewServer(registry),
	)

	shareddeps.StartGRPCServer()
}

