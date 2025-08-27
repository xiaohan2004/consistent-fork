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
	members := []consistent.Member{
		WeightedServer{name: "server1", weight: 3}, // High-performance server
		WeightedServer{name: "server2", weight: 2}, // Medium-performance server
		WeightedServer{name: "server3", weight: 1}, // Low-performance server
	}

	// Configure weighted consistent hash
	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 50,
		Load:              1,
		Hasher:            Hasher{},
	}

	// Create weighted consistent hash ring
	c := consistent.New(members, cfg)

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

	// Test key location distribution
	fmt.Println("\n=== Key Location Distribution Test ===")
	keyDistribution := make(map[string]int)
	for i := 0; i < 1000000; i++ {
		key := fmt.Sprintf("key-%d", i)
		member := c.LocateKey([]byte(key))
		keyDistribution[member.String()]++
	}
	for member, count := range keyDistribution {
		weight := weights[member]
		ratio := float64(count) / float64(weight)
		fmt.Printf("Member: %s, Key Count: %d, Weight: %d, Ratio: %.2f\n",
			member, count, weight, ratio)
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

	// Test adding some more servers
	fmt.Println("\n=== Adding More Servers ===")
	moreServers := []WeightedServer{
		{name: "server5", weight: 2},
		{name: "server6", weight: 3},
		{name: "server7", weight: 1},
		{name: "server8", weight: 4},
		{name: "server9", weight: 2},
		{name: "server10", weight: 3},
	}
	for _, server := range moreServers {
		c.Add(server)
	}
	fmt.Printf("New Total Weight: %d\n", c.GetTotalWeight())

	// Show updated load distribution
	fmt.Println("\n=== Updated Load Distribution After Adding More Servers ===")
	loads = c.LoadDistribution()
	weights = c.WeightDistribution()
	for member, load := range loads {
		weight := weights[member]
		ratio := load / float64(weight)
		fmt.Printf("Member: %s, Load: %.0f, Weight: %d, Ratio: %.2f\n",
			member, load, weight, ratio)
	}

	// Test key location distribution after adding more servers
	fmt.Println("\n=== Key Location Distribution After Adding More Servers ===")
	keyDistribution = make(map[string]int)
	for i := 0; i < 1000000; i++ {
		key := fmt.Sprintf("key-%d", i)
		member := c.LocateKey([]byte(key))
		keyDistribution[member.String()]++
	}
	for member, count := range keyDistribution {
		weight := weights[member]
		ratio := float64(count) / float64(weight)
		fmt.Printf("Member: %s, Key Count: %d, Weight: %d, Ratio: %.2f\n",
			member, count, weight, ratio)
	}

	// Test removing some servers
	fmt.Println("\n=== Removing Some Servers ===")
	serversToRemove := []string{"server2", "server5", "server8"}
	for _, name := range serversToRemove {
		c.Remove(name)
		fmt.Printf("Removed: %s\n", name)
	}
	fmt.Printf("New Total Weight: %d\n", c.GetTotalWeight())

	// Show updated load distribution
	fmt.Println("\n=== Updated Load Distribution After Removing Servers ===")
	loads = c.LoadDistribution()
	weights = c.WeightDistribution()
	for member, load := range loads {
		weight := weights[member]
		ratio := load / float64(weight)
		fmt.Printf("Member: %s, Load: %.0f, Weight: %d, Ratio: %.2f\n",
			member, load, weight, ratio)
	}

	// Test key location distribution after removing some servers
	fmt.Println("\n=== Key Location Distribution After Removing Servers ===")
	keyDistribution = make(map[string]int)
	for i := 0; i < 1000000; i++ {
		key := fmt.Sprintf("key-%d", i)
		member := c.LocateKey([]byte(key))
		keyDistribution[member.String()]++
	}
	for member, count := range keyDistribution {
		weight := weights[member]
		ratio := float64(count) / float64(weight)
		fmt.Printf("Member: %s, Key Count: %d, Weight: %d, Ratio: %.2f\n",
			member, count, weight, ratio)
	}
	fmt.Println("\n=== End of Example ===")
}
