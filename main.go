package main

import (
	"artifact-registry/config"
	"artifact-registry/orm"
	proto "artifact-registry/proto_gen"
	"artifact-registry/registry"
	"artifact-registry/registry/filesystemRegistry"
	"artifact-registry/registry/s3"

	"github.com/EnclaveRunner/shareddeps"
	"github.com/rs/zerolog/log"
)

func main() {
	// initialize gRPC server
	shareddeps.InitGRPCServer(
		config.Cfg, "artifact-registry", "v0.1.0", config.Defaults...,
	)
	orm.InitDB()

	persister := initializeRegistryPersister()

	proto.RegisterRegistryServiceServer(
		shareddeps.GRPCServer,
		registry.NewServer(persister),
	)

	shareddeps.StartGRPCServer()
}

func initializeRegistryPersister() registry.Registry {
	var registry registry.Registry
	switch config.Cfg.Persistence.Type {
		case "filesystem":
			registry = initFilesystemRegistry()
		case "s3":
			registry = initS3Registry()
		default:
			log.Warn().Msgf("unknown persistence type '%s', defaulting to filesystem", config.Cfg.Persistence.Type)
			registry = initFilesystemRegistry()
	}

	return registry
}

func initFilesystemRegistry() registry.Registry {
	// Initialize filesystem registry
	storageDir := filesystemRegistry.GetStorageDir()
	fsRegistry, err := filesystemRegistry.New(storageDir)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize filesystem registry")
	}
	log.Info().
		Str("storage_dir", storageDir).
		Msg("filesystem registry initialized")

	return fsRegistry
}

func initS3Registry() registry.Registry {
	// Initialize s3 registry
	s3Registry, err := s3.New()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize s3 registry")
	}
	log.Info().Msg("s3 registry initialized")

	return s3Registry
}
