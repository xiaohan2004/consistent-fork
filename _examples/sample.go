package main

import (
	"fmt"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

// In your distributed system, you probably have a custom data type
// for your cluster members. Just add a String function to implement
// consistent.Member interface.
type myMember struct{
	name string
	weight int
}

func (m myMember) String() string {
	return m.name
}

func (m myMember) Weight() int {
	return m.weight
}

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func main() {
	// Create a new consistent instance
	cfg := consistent.Config{
		PartitionCount:    7,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := consistent.New(nil, cfg)

	// Add some members to the consistent hash table.
	// Add function calculates average load and distributes partitions over members
	node1 := myMember{
		name: "node1.olricmq.com",
		weight: 1,
	}
	c.Add(node1)

	node2 := myMember{
		name: "node2.olricmq.com",
		weight: 2,
	}
	c.Add(node2)

	key := []byte("my-key")
	// calculates partition id for the given key
	// partID := hash(key) % partitionCount
	// the partitions is already distributed among members by Add function.
	owner := c.LocateKey(key)
	fmt.Println(owner.String())
	// Prints node2.olricmq.com
}
