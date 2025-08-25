// Package consistent provides a weighted consistent hashing function with bounded loads.
// This implementation adds weight support on top of the original algorithm.
//
// Example Use:
//
//	cfg := consistent.WeightedConfig{
//		PartitionCount:    71,
//		ReplicationFactor: 20,
//		Load:              1.25,
//		Hasher:            hasher{},
//	}
//
// Create weighted members:
//
//	member1 := &MyWeightedMember{name: "server1", weight: 2}
//	member2 := &MyWeightedMember{name: "server2", weight: 1}
//	members := []WeightedMember{member1, member2}
//
// Now you can create a new WeightedConsistent instance:
//
//	c := consistent.NewWeighted(members, cfg)
//
// LocateKey works the same way but considers member weights:
//
//	key := []byte("my-key")
//	member := c.LocateKey(key)
package consistent

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
)

// WeightedMember interface represents a weighted member in consistent hash ring.
type WeightedMember interface {
	Member
	Weight() int
}

// WeightedConfig represents a structure to control weighted consistent package.
type WeightedConfig struct {
	// Hasher is responsible for generating unsigned, 64-bit hash of provided byte slice.
	Hasher Hasher

	// Keys are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big PartitionCount if you have
	// too many keys.
	PartitionCount int

	// Base replication factor. Members will have replicas = ReplicationFactor * Weight
	ReplicationFactor int

	// Load is used to calculate average load. See the code, the paper and Google's blog post to learn about it.
	Load float64
}

// WeightedConsistent holds the information about the weighted members of the consistent hash circle.
type WeightedConsistent struct {
	mu sync.RWMutex

	config         WeightedConfig
	hasher         Hasher
	sortedSet      []uint64
	partitionCount uint64
	loads          map[string]float64
	members        map[string]*WeightedMember
	weights        map[string]int
	totalWeight    int
	partitions     map[int]*WeightedMember
	ring           map[uint64]*WeightedMember
}

// NewWeighted creates and returns a new WeightedConsistent object.
func NewWeighted(members []WeightedMember, config WeightedConfig) *WeightedConsistent {
	if config.Hasher == nil {
		panic("Hasher cannot be nil")
	}
	if config.PartitionCount == 0 {
		config.PartitionCount = DefaultPartitionCount
	}
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = DefaultReplicationFactor
	}
	if config.Load == 0 {
		config.Load = DefaultLoad
	}

	c := &WeightedConsistent{
		config:         config,
		members:        make(map[string]*WeightedMember),
		weights:        make(map[string]int),
		partitionCount: uint64(config.PartitionCount),
		ring:           make(map[uint64]*WeightedMember),
	}

	c.hasher = config.Hasher
	for _, member := range members {
		c.add(member)
	}
	if members != nil {
		c.distributePartitions()
	}
	return c
}

// GetMembers returns a thread-safe copy of members. If there are no members, it returns an empty slice of WeightedMember.
func (c *WeightedConsistent) GetMembers() []WeightedMember {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy of member list.
	members := make([]WeightedMember, 0, len(c.members))
	for _, member := range c.members {
		members = append(members, *member)
	}
	return members
}

// AverageLoad exposes the current average load considering weights.
func (c *WeightedConsistent) AverageLoad() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.averageLoad()
}

func (c *WeightedConsistent) averageLoad() float64 {
	if len(c.members) == 0 || c.totalWeight == 0 {
		return 0
	}

	avgLoad := float64(c.partitionCount)/float64(c.totalWeight) * c.config.Load
	return math.Ceil(avgLoad)
}

func (c *WeightedConsistent) distributeWithLoad(partID, idx int, partitions map[int]*WeightedMember, loads map[string]float64) {
	avgLoad := c.averageLoad()
	var count int
	for {
		count++
		if count >= len(c.sortedSet) {
			// User needs to decrease partition count, increase member count or increase load factor.
			panic("not enough room to distribute partitions")
		}
		i := c.sortedSet[idx]
		member := *c.ring[i]
		memberWeight := float64(c.weights[member.String()])
		expectedLoad := avgLoad * memberWeight
		load := loads[member.String()]
		if load+1 <= expectedLoad {
			partitions[partID] = &member
			loads[member.String()]++
			return
		}
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
	}
}

func (c *WeightedConsistent) distributePartitions() {
	loads := make(map[string]float64)
	partitions := make(map[int]*WeightedMember)

	bs := make([]byte, 8)
	for partID := uint64(0); partID < c.partitionCount; partID++ {
		binary.LittleEndian.PutUint64(bs, partID)
		key := c.hasher.Sum64(bs)
		idx := sort.Search(len(c.sortedSet), func(i int) bool {
			return c.sortedSet[i] >= key
		})
		if idx >= len(c.sortedSet) {
			idx = 0
		}
		c.distributeWithLoad(int(partID), idx, partitions, loads)
	}
	c.partitions = partitions
	c.loads = loads
}

func (c *WeightedConsistent) add(member WeightedMember) {
	weight := member.Weight()
	if weight <= 0 {
		weight = 1 // Ensure minimum weight of 1
	}

	// Calculate replicas based on weight
	replicas := c.config.ReplicationFactor * weight

	for i := 0; i < replicas; i++ {
		key := []byte(fmt.Sprintf("%s%d", member.String(), i))
		h := c.hasher.Sum64(key)
		c.ring[h] = &member
		c.sortedSet = append(c.sortedSet, h)
	}
	// sort hashes ascendingly
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})

	// Store member and weight information
	c.members[member.String()] = &member
	c.weights[member.String()] = weight
	c.totalWeight += weight
}

// Add adds a new weighted member to the consistent hash circle.
func (c *WeightedConsistent) Add(member WeightedMember) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.members[member.String()]; ok {
		// We already have this member. Quit immediately.
		return
	}
	c.add(member)
	c.distributePartitions()
}

func (c *WeightedConsistent) delSlice(val uint64) {
	for i := 0; i < len(c.sortedSet); i++ {
		if c.sortedSet[i] == val {
			c.sortedSet = append(c.sortedSet[:i], c.sortedSet[i+1:]...)
			break
		}
	}
}

// Remove removes a weighted member from the consistent hash circle.
func (c *WeightedConsistent) Remove(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.members[name]
	if !ok {
		// There is no member with that name. Quit immediately.
		return
	}

	weight := c.weights[name]
	replicas := c.config.ReplicationFactor * weight

	for i := 0; i < replicas; i++ {
		key := []byte(fmt.Sprintf("%s%d", name, i))
		h := c.hasher.Sum64(key)
		delete(c.ring, h)
		c.delSlice(h)
	}

	delete(c.members, name)
	c.totalWeight -= c.weights[name]
	delete(c.weights, name)

	if len(c.members) == 0 {
		// consistent hash ring is empty now. Reset the partition table.
		c.partitions = make(map[int]*WeightedMember)
		c.totalWeight = 0
		return
	}
	c.distributePartitions()
}

// LoadDistribution exposes load distribution of weighted members.
func (c *WeightedConsistent) LoadDistribution() map[string]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy
	res := make(map[string]float64)
	for member, load := range c.loads {
		res[member] = load
	}
	return res
}

// WeightDistribution exposes weight distribution of members.
func (c *WeightedConsistent) WeightDistribution() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy
	res := make(map[string]int)
	for member, weight := range c.weights {
		res[member] = weight
	}
	return res
}

// FindPartitionID returns partition id for given key.
func (c *WeightedConsistent) FindPartitionID(key []byte) int {
	hkey := c.hasher.Sum64(key)
	return int(hkey % c.partitionCount)
}

// GetPartitionOwner returns the owner of the given partition.
func (c *WeightedConsistent) GetPartitionOwner(partID int) WeightedMember {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.getPartitionOwner(partID)
}

// getPartitionOwner returns the owner of the given partition. It's not thread-safe.
func (c *WeightedConsistent) getPartitionOwner(partID int) WeightedMember {
	member, ok := c.partitions[partID]
	if !ok {
		return nil
	}
	// Create a thread-safe copy of member and return it.
	return *member
}

// LocateKey finds a home for given key considering member weights
func (c *WeightedConsistent) LocateKey(key []byte) WeightedMember {
	partID := c.FindPartitionID(key)
	return c.GetPartitionOwner(partID)
}

func (c *WeightedConsistent) getClosestN(partID, count int) ([]WeightedMember, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var res []WeightedMember
	if count > len(c.members) {
		return res, ErrInsufficientMemberCount
	}

	var ownerKey uint64
	owner := c.getPartitionOwner(partID)
	// Hash and sort all the names.
	var keys []uint64
	kmems := make(map[uint64]*WeightedMember)
	for name, member := range c.members {
		key := c.hasher.Sum64([]byte(name))
		if name == owner.String() {
			ownerKey = key
		}
		keys = append(keys, key)
		kmems[key] = member
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Find the key owner
	idx := 0
	for idx < len(keys) {
		if keys[idx] == ownerKey {
			key := keys[idx]
			res = append(res, *kmems[key])
			break
		}
		idx++
	}

	// Find the closest(replica owners) members.
	for len(res) < count {
		idx++
		if idx >= len(keys) {
			idx = 0
		}
		key := keys[idx]
		res = append(res, *kmems[key])
	}
	return res, nil
}

// GetClosestN returns the closest N weighted member to a key in the hash ring.
// This may be useful to find members for replication.
func (c *WeightedConsistent) GetClosestN(key []byte, count int) ([]WeightedMember, error) {
	partID := c.FindPartitionID(key)
	return c.getClosestN(partID, count)
}

// GetClosestNForPartition returns the closest N weighted member for given partition.
// This may be useful to find members for replication.
func (c *WeightedConsistent) GetClosestNForPartition(partID, count int) ([]WeightedMember, error) {
	return c.getClosestN(partID, count)
}

// GetTotalWeight returns the total weight of all members.
func (c *WeightedConsistent) GetTotalWeight() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalWeight
}
