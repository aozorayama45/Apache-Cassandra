package main

import (
	"testing"
	"time"
)

func TestDynamicCompactionThrottling(t *testing.T) {
	config := &Config{
		DynamicCompactionThrottlingEnabled: true,
		CompactionLatencyThresholdMs:       10.0,
		MinCompactionThroughputMbPerSec:    16,
		CompactionThroughputMbPerSec:       64,
	}

	metrics := &Metrics{readLatency: 5.0}
	jmxMetrics := &JMXMetrics{activeCompactionThroughputMb: config.CompactionThroughputMbPerSec}

	cm := NewCompactionManager(config, metrics, jmxMetrics)

	// Initial limit should be the ceiling limit
	if cm.rateLimiter.GetLimit() != 64 {
		t.Errorf("Expected initial limit to be 64, got %d", cm.rateLimiter.GetLimit())
	}

	// 1. Test scaling down when latency is high
	metrics.SetReadLatency(20.0) // 20ms is above 10ms threshold
	cm.adjustThrottling()

	// Expected limit: 64 * (10 / 20) = 32 MB/s
	expectedLimit := 32
	if cm.rateLimiter.GetLimit() != expectedLimit {
		t.Errorf("Expected limit to scale down to %d, got %d", expectedLimit, cm.rateLimiter.GetLimit())
	}

	// JMX metrics should be updated
	if jmxMetrics.GetActiveCompactionThroughputLimit() != expectedLimit {
		t.Errorf("Expected JMX metric to be %d, got %d", expectedLimit, jmxMetrics.GetActiveCompactionThroughputLimit())
	}

	// 2. Test scaling down to the floor limit (min_compaction_throughput_mb_per_sec)
	metrics.SetReadLatency(100.0) // Very high latency
	cm.adjustThrottling()

	// Expected limit should be clamped to min limit (16)
	if cm.rateLimiter.GetLimit() != 16 {
		t.Errorf("Expected limit to be clamped to min limit 16, got %d", cm.rateLimiter.GetLimit())
	}

	// 3. Test scaling back up when latency recovers
	metrics.SetReadLatency(5.0) // Below threshold
	cm.adjustThrottling()

	// Expected limit should increase by recovery step (4 MB/s), so 16 + 4 = 20
	if cm.rateLimiter.GetLimit() != 20 {
		t.Errorf("Expected limit to recover to 20, got %d", cm.rateLimiter.GetLimit())
	}

	// Adjust multiple times to recover fully to ceiling
	for i := 0; i < 15; i++ {
		cm.adjustThrottling()
	}

	if cm.rateLimiter.GetLimit() != 64 {
		t.Errorf("Expected limit to recover to ceiling 64, got %d", cm.rateLimiter.GetLimit())
	}
}

func TestDynamicCompactionThrottlingDisabled(t *testing.T) {
	config := &Config{
		DynamicCompactionThrottlingEnabled: false,
		CompactionLatencyThresholdMs:       10.0,
		MinCompactionThroughputMbPerSec:    16,
		CompactionThroughputMbPerSec:       64,
	}

	metrics := &Metrics{readLatency: 25.0}
	jmxMetrics := &JMXMetrics{activeCompactionThroughputMb: config.CompactionThroughputMbPerSec}

	cm := NewCompactionManager(config, metrics, jmxMetrics)
	cm.Start()
	defer cm.Stop()

	time.Sleep(1500 * time.Millisecond)

	// Limit should remain unchanged because throttling is disabled
	if cm.rateLimiter.GetLimit() != 64 {
		t.Errorf("Expected limit to remain 64 when disabled, got %d", cm.rateLimiter.GetLimit())
	}
}