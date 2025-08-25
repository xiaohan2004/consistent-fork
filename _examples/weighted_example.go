package main

import (
	"fmt"
	"log"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type Hasher struct{}

func (h Hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

// WeightedServer represents a server with weight
type WeightedServer struct {
	name   string
	weight int
}

func (s WeightedServer) String() string {
	return s.name
}

func (s WeightedServer) Weight() int {
	return s.weight
}

func main() {
	// Create weighted members
	members := []consistent.WeightedMember{
		WeightedServer{name: "server1", weight: 3}, // High-performance server
		WeightedServer{name: "server2", weight: 2}, // Medium-performance server
		WeightedServer{name: "server3", weight: 1}, // Low-performance server
	}

	// Configure weighted consistent hash
	cfg := consistent.WeightedConfig{
		PartitionCount:    71,
		ReplicationFactor: 10,
		Load:              1.25,
		Hasher:            Hasher{},
	}

	// Create weighted consistent hash ring
	c := consistent.NewWeighted(members, cfg)

	fmt.Println("=== Weighted Consistent Hash Example ===")
	fmt.Printf("Total Weight: %d\n", c.GetTotalWeight())
	fmt.Printf("Average Load: %.2f\n", c.AverageLoad())

	// Show weight distribution
	fmt.Println("\n=== Weight Distribution ===")
	weights := c.WeightDistribution()
	for member, weight := range weights {
		fmt.Printf("Member: %s, Weight: %d\n", member, weight)
	}

	// Show load distribution
	fmt.Println("\n=== Load Distribution ===")
	loads := c.LoadDistribution()
	for member, load := range loads {
		weight := weights[member]
		ratio := load / float64(weight)
		fmt.Printf("Member: %s, Load: %.0f, Weight: %d, Ratio: %.2f\n",
			member, load, weight, ratio)
	}

	// Test key location
	fmt.Println("\n=== Key Location Test ===")
	testKeys := []string{"key1", "key2", "key3", "key4", "key5", "user:123", "data:456"}

	for _, key := range testKeys {
		member := c.LocateKey([]byte(key))
		weight := weights[member.String()]
		fmt.Printf("Key: %s -> Server: %s (weight: %d)\n", key, member.String(), weight)
	}

	// Test replica finding
	fmt.Println("\n=== Replica Test ===")
	key := []byte("important-data")
	replicas, err := c.GetClosestN(key, 2)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Key: %s\n", string(key))
	fmt.Printf("Primary: %s (weight: %d)\n", replicas[0].String(), replicas[0].Weight())
	if len(replicas) > 1 {
		fmt.Printf("Replica: %s (weight: %d)\n", replicas[1].String(), replicas[1].Weight())
	}

	// Test adding a new server
	fmt.Println("\n=== Adding New Server ===")
	newServer := WeightedServer{name: "server4", weight: 4} // Very high-performance server
	c.Add(newServer)

	fmt.Printf("New Total Weight: %d\n", c.GetTotalWeight())

	// Show updated load distribution
	fmt.Println("\n=== Updated Load Distribution ===")
	loads = c.LoadDistribution()
	weights = c.WeightDistribution()
	for member, load := range loads {
		weight := weights[member]
		ratio := load / float64(weight)
		fmt.Printf("Member: %s, Load: %.0f, Weight: %d, Ratio: %.2f\n",
			member, load, weight, ratio)
	}

	// Test key location after adding new server
	fmt.Println("\n=== Key Location After Adding Server ===")
	for _, key := range testKeys {
		member := c.LocateKey([]byte(key))
		weight := weights[member.String()]
		fmt.Printf("Key: %s -> Server: %s (weight: %d)\n", key, member.String(), weight)
	}
}
