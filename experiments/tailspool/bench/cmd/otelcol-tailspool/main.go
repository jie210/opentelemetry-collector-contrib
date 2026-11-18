// Command otelcol-tailspool is a collector with the components needed to
// compare the in-memory tail sampling processor with its spool mode.
package main

import (
	"log"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/nopexporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tailspoolreceiver"
)

func components() (otelcol.Factories, error) {
	var err error
	f := otelcol.Factories{Telemetry: otelconftelemetry.NewFactory()}
	if f.Receivers, err = otelcol.MakeFactoryMap[receiver.Factory](otlpreceiver.NewFactory(), tailspoolreceiver.NewFactory()); err != nil {
		return f, err
	}
	if f.Processors, err = otelcol.MakeFactoryMap[processor.Factory](tailsamplingprocessor.NewFactory()); err != nil {
		return f, err
	}
	if f.Exporters, err = otelcol.MakeFactoryMap[exporter.Factory](otlpexporter.NewFactory(), nopexporter.NewFactory()); err != nil {
		return f, err
	}
	return f, nil
}

func main() {
	settings := otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{Command: "otelcol-tailspool", Description: "tail sampling spool benchmark collector", Version: "dev"},
		Factories: components,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory(), envprovider.NewFactory()},
			},
		},
	}
	if err := otelcol.NewCommand(settings).Execute(); err != nil {
		log.Fatal(err)
	}
}
