// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd)

package spool // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/spool"

import "os"

// lockDir does not lock on platforms without flock.
func lockDir(string) (*os.File, error) {
	return nil, nil
}
