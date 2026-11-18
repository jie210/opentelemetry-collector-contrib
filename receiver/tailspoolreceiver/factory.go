// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailspoolreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tailspoolreceiver"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

var componentType = component.MustNewType("tailspool")

// Config configures the gRPC server.
type Config struct {
	GRPC configgrpc.ServerConfig `mapstructure:"grpc"`
}

// NewFactory returns the factory of the tailspool receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		componentType,
		createDefaultConfig,
		receiver.WithTraces(createTraces, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	grpcCfg := configgrpc.NewDefaultServerConfig()
	grpcCfg.NetAddr.Endpoint = "localhost:4317"
	grpcCfg.ReadBufferSize = 512 * 1024
	return &Config{GRPC: grpcCfg}
}

func createTraces(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Traces) (receiver.Traces, error) {
	return newReceiver(cfg.(*Config), set, next), nil
}
