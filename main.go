package main

import (
	"fmt"
	"sync"
	"time"
)

// Config represents the configuration parameters, simulating cassandra.yaml
type Config struct {
	DynamicCompactionThrottlingEnabled bool
	CompactionLatencyThresholdMs       float64
	MinCompactionThroughputMbPerSec    int
	CompactionThroughputMbPerSec       int
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		DynamicCompactionThrottlingEnabled: true,
		CompactionLatencyThresholdMs:       10.0,
		MinCompactionThroughputMbPerSec:    16,
		CompactionThroughputMbPerSec:       64,
	}
}

// RateLimiter controls the throughput of compaction tasks
type RateLimiter struct {
	mu         sync.Mutex
	limitBytes float64 // bytes per second
	tokens     float64
	lastRefill time.Time
}

func NewRateLimiter(mbPerSec int) *RateLimiter {
	return &RateLimiter{
		limitBytes: float64(mbPerSec) * 1024 * 1024,
		tokens:     float64(mbPerSec) * 1024 * 1024,
		lastRefill: time.Now(),
	}
}

func (rl *RateLimiter) SetLimit(mbPerSec int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.limitBytes = float64(mbPerSec) * 1024 * 1024
	if rl.tokens > rl.limitBytes {
		rl.tokens = rl.limitBytes
	}
}

func (rl *RateLimiter) GetLimit() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return int(rl.limitBytes / (1024 * 1024))
}

func (rl *RateLimiter) Acquire(bytes int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for {
		now := time.Now()
		elapsed := now.Sub(rl.lastRefill).Seconds()
		rl.lastRefill = now

		rl.tokens += elapsed * rl.limitBytes
		if rl.tokens > rl.limitBytes {
			rl.tokens = rl.limitBytes
		}

		if rl.tokens >= float64(bytes) {
			rl.tokens -= float64(bytes)
			return
		}

		// Wait for tokens to accumulate
		needed := float64(bytes) - rl.tokens
		sleepTime := time.Duration((needed / rl.limitBytes) * float64(time.Second))
		rl.mu.Unlock()
		time.Sleep(sleepTime)
		rl.mu.Lock()
	}
}

// Metrics tracks system metrics like read latency
type Metrics struct {
	mu          sync.RWMutex
	readLatency float64 // in milliseconds
}

func (m *Metrics) GetReadLatency() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.readLatency
}

func (m *Metrics) SetReadLatency(latency float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readLatency = latency
}

// JMXMetrics exposes metrics for external monitoring
type JMXMetrics struct {
	mu                           sync.RWMutex
	activeCompactionThroughputMb int
}

func (jmx *JMXMetrics) GetActiveCompactionThroughputLimit() int {
	jmx.mu.RLock()
	defer jmx.mu.RUnlock()
	return jmx.activeCompactionThroughputMb
}

func (jmx *JMXMetrics) SetActiveCompactionThroughputLimit(limit int) {
	jmx.mu.Lock()
	defer jmx.mu.Unlock()
	jmx.activeCompactionThroughputMb = limit
}

// CompactionManager coordinates compaction tasks and dynamic throttling
type CompactionManager struct {
	config      *Config
	rateLimiter *RateLimiter
	metrics     *Metrics
	jmxMetrics  *JMXMetrics
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

func NewCompactionManager(config *Config, metrics *Metrics, jmxMetrics *JMXMetrics) *CompactionManager {
	return &CompactionManager{
		config:      config,
		rateLimiter: NewRateLimiter(config.CompactionThroughputMbPerSec),
		metrics:     metrics,
		jmxMetrics:  jmxMetrics,
		stopChan:    make(chan struct{}),
	}
}

func (cm *CompactionManager) Start() {
	if !cm.config.DynamicCompactionThrottlingEnabled {
		return
	}

	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cm.adjustThrottling()
			case <-cm.stopChan:
				return
			}
		}
	}
}

func (cm *CompactionManager) Stop() {
	select {
	case <-cm.stopChan:
		// Already closed
	default:
		close(cm.stopChan)
	}
	cm.wg.Wait()
}

func (cm *CompactionManager) adjustThrottling() { 
	currentLatency := cm.metrics.GetReadLatency()
	currentLimit := cm.rateLimiter.GetLimit()
	targetLimit := currentLimit

	threshold := cm.config.CompactionLatencyThresholdMs
	minLimit := cm.config.MinCompactionThroughputMbPerSec
	maxLimit := cm.config.CompactionThroughputMbPerSec

	if currentLatency > threshold {
		// Latency is elevated, scale down compaction throughput
		ratio := threshold / currentLatency
		calculatedLimit := int(float64(currentLimit) * ratio)
		if calculatedLimit < minLimit {
			targetLimit = minLimit
		} else {
			targetLimit = calculatedLimit
		}
	} else {
		// Latency is within acceptable limits, scale back up to the ceiling limit
		if currentLimit < maxLimit {
			recoveryStep := 4 // MB/s
			targetLimit = currentLimit + recoveryStep
			if targetLimit > maxLimit {
				targetLimit = maxLimit
			}
		}
	}

	if targetLimit != currentLimit {
		cm.rateLimiter.SetLimit(targetLimit)
		cm.jmxMetrics.SetActiveCompactionThroughputLimit(targetLimit)
		fmt.Printf("[CompactionManager] Read Latency: %.2f ms (Threshold: %.2f ms) -> Adjusted Compaction Throughput Limit to %d MB/s\n",
			currentLatency, threshold, targetLimit)
	}
}

func (cm *CompactionManager) PerformCompaction(taskID int, dataSizeMb int) {
	fmt.Printf("[CompactionTask-%d] Starting compaction of %d MB...\n", taskID, dataSizeMb)
	remainingBytes := dataSizeMb * 1024 * 1024
	chunkSize := 1 * 1024 * 1024 // 1 MB chunks

	for remainingBytes > 0 {
		toProcess := chunkSize
		if remainingBytes < chunkSize {
			toProcess = remainingBytes
		}

		cm.rateLimiter.Acquire(toProcess)
		remainingBytes -= toProcess
	}
	fmt.Printf("[CompactionTask-%d] Compaction completed.\n", taskID)
}

func main() {
	fmt.Println("Starting Apache Cassandra Compaction Throttling Simulation...")

	config := DefaultConfig()
	metrics := &Metrics{readLatency: 5.0} // Start with low latency
	jmxMetrics := &JMXMetrics{activeCompactionThroughputMb: config.CompactionThroughputMbPerSec}

	cm := NewCompactionManager(config, metrics, jmxMetrics)
	cm.Start()
	defer cm.Stop()

	// Run a background compaction task
	go func() {
		for i := 1; i <= 3; i++ {
			cm.PerformCompaction(i, 10) // Compact 10 MB
			time.Sleep(1 * time.Second)
		}
	}()

	// Simulate changing read latency over time
	time.Sleep(1500 * time.Millisecond)
	fmt.Println("\n--- Simulating High Read Latency (Spike to 25ms) ---")
	metrics.SetReadLatency(25.0)

	time.Sleep(4 * time.Second)
	fmt.Println("\n--- Simulating Read Latency Recovery (Drop to 4ms) ---")
	metrics.SetReadLatency(4.0)

	time.Sleep(5 * time.Second)
	fmt.Println("Simulation finished.")
}