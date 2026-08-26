package outbox

import (
	"hash/fnv"
)

// JumpConsistentHash maps a uint64 key to a bucket in [0, numBuckets) using
// Google's jump consistent hash. It minimizes key reshuffles when the number
// of buckets changes, which is exactly what we need for outbox sharding.
func JumpConsistentHash(key uint64, numBuckets int) int {
	if numBuckets <= 0 {
		return 0
	}
	var b int64 = -1
	var j int64
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(1<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

// HashTenant returns a 64-bit FNV-1a hash of the tenant ID.
func HashTenant(tenantID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tenantID))
	return h.Sum64()
}

// PartitionForTenant returns the stable partition (shard) for a tenant.
func PartitionForTenant(tenantID string, shardCount int) int {
	if shardCount <= 0 {
		return 0
	}
	return JumpConsistentHash(HashTenant(tenantID), shardCount)
}

// OwnsPartition reports whether the given owned-shard list includes partition.
func OwnsPartition(partition int, ownedShards []int) bool {
	for _, s := range ownedShards {
		if s == partition {
			return true
		}
	}
	return false
}
