// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !(darwin || freebsd || linux)

package spool // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"

import "errors"

func freeDiskSpace(string) (int64, error) {
	return 0, errors.New("free disk space is not available on this platform")
}
