package main

import (
	"artifact-registry/config"
	"artifact-registry/orm"
	proto "artifact-registry/proto_gen"
	"artifact-registry/registry"
	"artifact-registry/registry/filesystemRegistry"

	"github.com/EnclaveRunner/shareddeps"
	"github.com/rs/zerolog/log"
)

func main() {
	cfg := &config.AppConfig{}
	shareddeps.PopulateAppConfig(
		cfg, "artifact-registry", "v0.4.0", config.Defaults...,
	)

	// initialize gRPC server
	server := shareddeps.InitGRPCServer()
	db := orm.InitDB(cfg)
	// Initialize filesystem registry
	storageDir := filesystemRegistry.GetStorageDir(cfg)
	fsRegistry, err := filesystemRegistry.New(storageDir)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize filesystem registry")
	}

	log.Info().
		Str("storage_dir", storageDir).
		Msg("Filesystem registry initialized")

	proto.RegisterRegistryServiceServer(
		server,
		registry.NewServer(fsRegistry, db),
	)

	shareddeps.StartGRPCServer(cfg, server)
}
