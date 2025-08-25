package consistent

import (
	"hash/fnv"
	"fmt"
	"testing"
)

// Test hasher for weighted consistent tests
type testWeightedHasher struct{}

func (hs testWeightedHasher) Sum64(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

// Test weighted member implementation
type testWeightedMember struct {
	name   string
	weight int
}

func (m testWeightedMember) String() string {
	return m.name
}

func (m testWeightedMember) Weight() int {
	return m.weight
}

func TestWeightedConsistent_New(t *testing.T) {
	members := []WeightedMember{
		testWeightedMember{name: "server1", weight: 2},
		testWeightedMember{name: "server2", weight: 1},
	}

	cfg := WeightedConfig{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            testWeightedHasher{},
	}

	c := NewWeighted(members, cfg)

	if c == nil {
		t.Fatal("NewWeighted returned nil")
	}

	if len(c.GetMembers()) != 2 {
		t.Fatalf("Expected 2 members, got %d", len(c.GetMembers()))
	}

	if c.GetTotalWeight() != 3 {
		t.Fatalf("Expected total weight 3, got %d", c.GetTotalWeight())
	}
}

func TestWeightedConsistent_Add(t *testing.T) {
	cfg := WeightedConfig{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            testWeightedHasher{},
	}

	c := NewWeighted(nil, cfg)

	member1 := testWeightedMember{name: "server1", weight: 2}
	member2 := testWeightedMember{name: "server2", weight: 3}

	c.Add(member1)
	if len(c.GetMembers()) != 1 {
		t.Fatalf("Expected 1 member after first add, got %d", len(c.GetMembers()))
	}
	if c.GetTotalWeight() != 2 {
		t.Fatalf("Expected total weight 2, got %d", c.GetTotalWeight())
	}

	c.Add(member2)
	if len(c.GetMembers()) != 2 {
		t.Fatalf("Expected 2 members after second add, got %d", len(c.GetMembers()))
	}
	if c.GetTotalWeight() != 5 {
		t.Fatalf("Expected total weight 5, got %d", c.GetTotalWeight())
	}

	// Try adding the same member again
	c.Add(member1)
	if len(c.GetMembers()) != 2 {
		t.Fatalf("Expected 2 members after duplicate add, got %d", len(c.GetMembers()))
	}
	if c.GetTotalWeight() != 5 {
		t.Fatalf("Expected total weight 5 after duplicate add, got %d", c.GetTotalWeight())
	}
}

func TestWeightedConsistent_Remove(t *testing.T) {
	members := []WeightedMember{
		testWeightedMember{name: "server1", weight: 2},
		testWeightedMember{name: "server2", weight: 3},
		testWeightedMember{name: "server3", weight: 1},
	}

	cfg := WeightedConfig{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            testWeightedHasher{},
	}

	c := NewWeighted(members, cfg)

	if c.GetTotalWeight() != 6 {
		t.Fatalf("Expected initial total weight 6, got %d", c.GetTotalWeight())
	}

	c.Remove("server2")
	if len(c.GetMembers()) != 2 {
		t.Fatalf("Expected 2 members after remove, got %d", len(c.GetMembers()))
	}
	if c.GetTotalWeight() != 3 {
		t.Fatalf("Expected total weight 3 after remove, got %d", c.GetTotalWeight())
	}

	// Try removing non-existent member
	c.Remove("nonexistent")
	if len(c.GetMembers()) != 2 {
		t.Fatalf("Expected 2 members after removing nonexistent, got %d", len(c.GetMembers()))
	}
	if c.GetTotalWeight() != 3 {
		t.Fatalf("Expected total weight 3 after removing nonexistent, got %d", c.GetTotalWeight())
	}

	// Remove all members
	c.Remove("server1")
	c.Remove("server3")
	if len(c.GetMembers()) != 0 {
		t.Fatalf("Expected 0 members after removing all, got %d", len(c.GetMembers()))
	}
	if c.GetTotalWeight() != 0 {
		t.Fatalf("Expected total weight 0 after removing all, got %d", c.GetTotalWeight())
	}
}

func TestWeightedConsistent_LocateKey(t *testing.T) {
	members := []WeightedMember{
		testWeightedMember{name: "server1", weight: 2},
		testWeightedMember{name: "server2", weight: 1},
	}

	cfg := WeightedConfig{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            testWeightedHasher{},
	}

	c := NewWeighted(members, cfg)

	key := []byte("test-key")
	member := c.LocateKey(key)

	if member == nil {
		t.Fatal("LocateKey returned nil")
	}

	// The same key should always map to the same member
	for i := 0; i < 10; i++ {
		if c.LocateKey(key).String() != member.String() {
			t.Fatal("LocateKey returned different members for the same key")
		}
	}
}

func TestWeightedConsistent_GetClosestN(t *testing.T) {
	members := []WeightedMember{
		testWeightedMember{name: "server1", weight: 2},
		testWeightedMember{name: "server2", weight: 1},
		testWeightedMember{name: "server3", weight: 3},
	}

	cfg := WeightedConfig{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            testWeightedHasher{},
	}

	c := NewWeighted(members, cfg)

	key := []byte("test-key")
	closest, err := c.GetClosestN(key, 2)

	if err != nil {
		t.Fatalf("GetClosestN returned error: %v", err)
	}

	if len(closest) != 2 {
		t.Fatalf("Expected 2 closest members, got %d", len(closest))
	}

	// Test error case - requesting more members than available
	_, err = c.GetClosestN(key, 5)
	if err != ErrInsufficientMemberCount {
		t.Fatalf("Expected ErrInsufficientMemberCount, got %v", err)
	}
}

func TestWeightedConsistent_LoadDistribution(t *testing.T) {
	members := []WeightedMember{
		testWeightedMember{name: "server1", weight: 2},
		testWeightedMember{name: "server2", weight: 1},
	}

	cfg := WeightedConfig{
		PartitionCount:    100,
		ReplicationFactor: 10,
		Load:              1.0,
		Hasher:            testWeightedHasher{},
	}

	c := NewWeighted(members, cfg)

	loads := c.LoadDistribution()
	weights := c.WeightDistribution()

	if len(loads) != 2 {
		t.Fatalf("Expected 2 entries in load distribution, got %d", len(loads))
	}

	// Check that higher weight member gets more load
	server1Load := loads["server1"]
	server2Load := loads["server2"]
	server1Weight := float64(weights["server1"])
	server2Weight := float64(weights["server2"])

	// The load ratio should roughly match the weight ratio
	server1Ratio := server1Load / server1Weight
	server2Ratio := server2Load / server2Weight

	// Allow some tolerance due to distribution algorithm
	tolerance := 2.0 // Increased tolerance for small partition counts
	if abs(server1Ratio-server2Ratio) > tolerance {
		t.Logf("Server1: load=%.0f, weight=%.0f, ratio=%.2f", server1Load, server1Weight, server1Ratio)
		t.Logf("Server2: load=%.0f, weight=%.0f, ratio=%.2f", server2Load, server2Weight, server2Ratio)
		t.Errorf("Load distribution doesn't respect weights properly. Ratio difference: %.2f", abs(server1Ratio-server2Ratio))
	}
}

func TestWeightedConsistent_WeightDistribution(t *testing.T) {
	members := []WeightedMember{
		testWeightedMember{name: "server1", weight: 2},
		testWeightedMember{name: "server2", weight: 3},
	}

	cfg := WeightedConfig{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            testWeightedHasher{},
	}

	c := NewWeighted(members, cfg)

	weights := c.WeightDistribution()

	if len(weights) != 2 {
		t.Fatalf("Expected 2 entries in weight distribution, got %d", len(weights))
	}

	if weights["server1"] != 2 {
		t.Fatalf("Expected server1 weight 2, got %d", weights["server1"])
	}

	if weights["server2"] != 3 {
		t.Fatalf("Expected server2 weight 3, got %d", weights["server2"])
	}
}

func TestWeightedConsistent_ZeroWeight(t *testing.T) {
	members := []WeightedMember{
		testWeightedMember{name: "server1", weight: 0}, // Zero weight should be treated as 1
	}

	cfg := WeightedConfig{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            testWeightedHasher{},
	}

	c := NewWeighted(members, cfg)

	weights := c.WeightDistribution()
	if weights["server1"] != 1 {
		t.Fatalf("Expected zero weight to be treated as 1, got %d", weights["server1"])
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// Benchmark weighted consistent hash performance
func BenchmarkWeightedConsistent_LocateKey(b *testing.B) {
	members := make([]WeightedMember, 0, 100)
	for i := 0; i < 100; i++ {
		weight := (i % 5) + 1 // Weights from 1 to 5
		members = append(members, testWeightedMember{
			name:   fmt.Sprintf("server%d", i),
			weight: weight,
		})
	}

	cfg := WeightedConfig{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            testWeightedHasher{},
	}

	c := NewWeighted(members, cfg)
	key := []byte("benchmark-key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.LocateKey(key)
	}
}
