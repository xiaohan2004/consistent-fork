package consistent

import (
	"fmt"
	"hash/fnv"
	"testing"
)

// Test hasher implementation
type testHasher struct{}

func (h testHasher) Sum64(data []byte) uint64 {
	hasher := fnv.New64a()
	hasher.Write(data)
	return hasher.Sum64()
}

// Test weighted member implementation for wrapper
type wrapperTestMember struct {
	name   string
	weight int
}

func (m *wrapperTestMember) String() string {
	return m.name
}

func (m *wrapperTestMember) Weight() int {
	return m.weight
}

func TestWeightedWrapper(t *testing.T) {
	members := []WeightedMember{
		&wrapperTestMember{name: "server1", weight: 3},
		&wrapperTestMember{name: "server2", weight: 1},
		&wrapperTestMember{name: "server3", weight: 2},
	}

	config := Config{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            testHasher{},
	}

	wrapper := NewWeightedWrapper(members, config)

	// Test that we can locate keys
	key := []byte("test-key")
	member := wrapper.LocateKeyWeighted(key)
	if member == nil {
		t.Fatal("Expected to find a member for key")
	}

	// Test weights are preserved
	weights := wrapper.GetWeights()
	if weights["server1"] != 3 {
		t.Errorf("Expected server1 weight to be 3, got %d", weights["server1"])
	}
	if weights["server2"] != 1 {
		t.Errorf("Expected server2 weight to be 1, got %d", weights["server2"])
	}
	if weights["server3"] != 2 {
		t.Errorf("Expected server3 weight to be 2, got %d", weights["server3"])
	}

	// Test getting weighted members
	weightedMembers := wrapper.GetWeightedMembers()
	if len(weightedMembers) != 3 {
		t.Errorf("Expected 3 weighted members, got %d", len(weightedMembers))
	}

	// Test adding a member
	newMember := &wrapperTestMember{name: "server4", weight: 4}
	wrapper.AddWeighted(newMember)

	weights = wrapper.GetWeights()
	if weights["server4"] != 4 {
		t.Errorf("Expected server4 weight to be 4, got %d", weights["server4"])
	}

	// Test removing a member
	wrapper.RemoveWeighted("server2")
	weights = wrapper.GetWeights()
	if _, exists := weights["server2"]; exists {
		t.Error("Expected server2 to be removed")
	}

	// Test that we can still locate keys after changes
	member = wrapper.LocateKeyWeighted(key)
	if member == nil {
		t.Fatal("Expected to find a member for key after changes")
	}
}

func TestWeightedWrapperDistribution(t *testing.T) {
	members := []WeightedMember{
		&wrapperTestMember{name: "heavy", weight: 10}, // Should get more keys
		&wrapperTestMember{name: "light", weight: 1},  // Should get fewer keys
	}

	config := Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            testHasher{},
	}

	wrapper := NewWeightedWrapper(members, config)

	// Test distribution with many keys
	distribution := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		member := wrapper.LocateKeyWeighted(key)
		if member != nil {
			distribution[member.String()]++
		}
	}

	heavyCount := distribution["heavy"]
	lightCount := distribution["light"]

	// Heavy server should get significantly more keys (roughly 10x more)
	if heavyCount <= lightCount {
		t.Errorf("Heavy server should get more keys. Heavy: %d, Light: %d", heavyCount, lightCount)
	}

	// The ratio should be roughly proportional to the weights (10:1)
	ratio := float64(heavyCount) / float64(lightCount)
	if ratio < 5.0 { // Allow some variance, but should be at least 5x
		t.Errorf("Expected ratio of at least 5:1, got %.2f:1", ratio)
	}
}
