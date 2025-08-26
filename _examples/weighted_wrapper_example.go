package main

import (
	"fmt"
	"hash/fnv"

	"github.com/buraksezer/consistent"
)

// Simple hasher implementation
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	hasher := fnv.New64a()
	hasher.Write(data)
	return hasher.Sum64()
}

// Example weighted member implementation
type WeightedServer struct {
	name   string
	weight int
}

func (s *WeightedServer) String() string {
	return s.name
}

func (s *WeightedServer) Weight() int {
	return s.weight
}

func main() {
	// Create weighted members
	members := []consistent.WeightedMember{
		&WeightedServer{name: "server1", weight: 3}, // High weight
		&WeightedServer{name: "server2", weight: 1}, // Low weight
		&WeightedServer{name: "server3", weight: 2}, // Medium weight
	}

	// Create config
	config := consistent.Config{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            hasher{},
	}

	// Create weighted wrapper
	ring := consistent.NewWeightedWrapper(members, config)

	// Test key distribution
	fmt.Println("Testing key distribution:")
	keys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10"}

	distribution := make(map[string]int)
	for _, key := range keys {
		member := ring.LocateKeyWeighted([]byte(key))
		if member != nil {
			distribution[member.String()]++
			fmt.Printf("Key '%s' -> %s (weight: %d)\n", key, member.String(), member.Weight())
		}
	}

	fmt.Println("\nDistribution summary:")
	for server, count := range distribution {
		fmt.Printf("%s: %d keys\n", server, count)
	}

	fmt.Println("\nWeight distribution:")
	weights := ring.GetWeights()
	for server, weight := range weights {
		fmt.Printf("%s: weight %d\n", server, weight)
	}

	// Test adding a new weighted member
	fmt.Println("\nAdding new server with weight 4...")
	newServer := &WeightedServer{name: "server4", weight: 4}
	ring.AddWeighted(newServer)

	fmt.Println("Updated weight distribution:")
	weights = ring.GetWeights()
	for server, weight := range weights {
		fmt.Printf("%s: weight %d\n", server, weight)
	}

	// Test removal
	fmt.Println("\nRemoving server2...")
	ring.RemoveWeighted("server2")

	fmt.Println("Final weight distribution:")
	weights = ring.GetWeights()
	for server, weight := range weights {
		fmt.Printf("%s: weight %d\n", server, weight)
	}
}
