// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// SpoolConfig configures the on-disk spool.
type SpoolConfig struct {
	// Directory holds the segment files and the log. It must not be shared.
	Directory string `mapstructure:"directory"`
	// SegmentSize is the size in bytes after which a new segment file is
	// started. Defaults to 64 MiB.
	SegmentSize int64 `mapstructure:"segment_size"`
	// CheckpointSize is the log size in bytes that triggers a compacting
	// checkpoint. Defaults to 64 MiB.
	CheckpointSize int64 `mapstructure:"checkpoint_size"`
	// SyncOnRotate fsyncs a segment file when it is sealed.
	SyncOnRotate bool `mapstructure:"sync_on_rotate"`
	// MaxInflightRecords bounds the requests handed to the next consumer and
	// not yet committed; it bounds redelivery after a process kill.
	// Defaults to 16.
	MaxInflightRecords int `mapstructure:"max_inflight_records"`
	// MaxInflightBytes bounds the stored bytes of those requests. Defaults to
	// 64 MiB.
	MaxInflightBytes int64 `mapstructure:"max_inflight_bytes"`
	// MinFreeDisk refuses new requests with a retryable error while the
	// directory's file system has less free space in bytes. Zero disables
	// the check.
	MinFreeDisk int64 `mapstructure:"min_free_disk"`
	// MinDeadlineRemaining refuses requests whose deadline is closer when
	// they are about to be written, as the client may give up before the
	// response arrives and retry. Zero only refuses expired requests.
	MinDeadlineRemaining time.Duration `mapstructure:"min_deadline_remaining"`
	// ReadRetries is how many times a stored request that failed to read
	// back is read again, with a growing delay, before its spans are
	// dropped. Zero drops them at the first failure.
	ReadRetries int `mapstructure:"read_retries"`
}

func (cfg *SpoolConfig) withDefaults() SpoolConfig {
	out := *cfg
	if out.SegmentSize <= 0 {
		out.SegmentSize = 64 << 20
	}
	if out.CheckpointSize <= 0 {
		out.CheckpointSize = 64 << 20
	}
	if out.MaxInflightRecords <= 0 {
		out.MaxInflightRecords = 16
	}
	if out.MaxInflightBytes <= 0 {
		out.MaxInflightBytes = 64 << 20
	}
	return out
}

func (cfg *Config) validateSpool() error {
	if cfg.Spool.Directory == "" {
		return errors.New("spool.directory must be set")
	}
	if cfg.NumShards > 1 {
		return errors.New("spool does not support num_shards greater than 1")
	}
	if cfg.SamplingStrategy != samplingStrategyTraceComplete {
		return fmt.Errorf("spool only supports sampling_strategy %q", samplingStrategyTraceComplete)
	}
	if cfg.TailStorageID != nil {
		return errors.New("spool cannot be combined with tail_storage")
	}
	if cfg.Spool.ReadRetries < 0 {
		return errors.New("spool.read_retries must not be negative")
	}
	if cfg.BlockOnOverflow {
		// A decided trace only leaves memory once its decision is cached, so
		// a disabled cache would make waiting for space block forever.
		if cfg.DecisionCache.SampledCacheSize <= 0 {
			return errors.New("spool with block_on_overflow requires decision_cache.sampled_cache_size greater than 0")
		}
		if cfg.DecisionCache.NonSampledCacheSize <= 0 {
			return errors.New("spool with block_on_overflow requires decision_cache.non_sampled_cache_size greater than 0")
		}
	}
	for _, p := range cfg.PolicyCfgs {
		if err := checkFoldablePolicy(p); err != nil {
			return fmt.Errorf("policy %q: %w", p.Name, err)
		}
	}
	return nil
}

// checkFoldablePolicy rejects OTTL conditions whose result depends on when
// they run, since the spool evaluates them when spans arrive.
func checkFoldablePolicy(p PolicyCfg) error {
	shared := []sharedPolicyCfg{p.sharedPolicyCfg, p.NotCfg.SubPolicy.sharedPolicyCfg}
	for _, sub := range p.CompositeCfg.SubPolicyCfg {
		shared = append(shared, sub.sharedPolicyCfg)
		for _, and := range sub.AndCfg.SubPolicyCfg {
			shared = append(shared, and.sharedPolicyCfg, and.NotCfg.SubPolicy.sharedPolicyCfg)
		}
	}
	for _, subs := range [][]AndSubPolicyCfg{p.AndCfg.SubPolicyCfg, p.DropCfg.SubPolicyCfg} {
		for _, sub := range subs {
			shared = append(shared, sub.sharedPolicyCfg, sub.NotCfg.SubPolicy.sharedPolicyCfg)
		}
	}
	for _, s := range shared {
		for _, cond := range append(append([]string{}, s.OTTLConditionCfg.SpanConditions...), s.OTTLConditionCfg.SpanEventConditions...) {
			if strings.Contains(cond, "Now(") {
				return fmt.Errorf("ottl condition %q depends on the current time", cond)
			}
		}
	}
	return nil
}
