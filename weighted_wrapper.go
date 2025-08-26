// Package consistent provides a weighted wrapper around the base consistent hashing implementation.
package consistent

import (
	"fmt"
)

// WeightedWrapper wraps the base Consistent struct to provide weighted functionality.
type WeightedWrapper struct {
	*Consistent
	weights map[string]int
}

// NewWeightedWrapper creates a new weighted consistent hash ring by wrapping the base implementation.
func NewWeightedWrapper(members []WeightedMember, config Config) *WeightedWrapper {
	// Convert weighted members to regular members with weight-based replication
	var expandedMembers []Member
	weights := make(map[string]int)

	for _, wmember := range members {
		weight := wmember.Weight()
		if weight <= 0 {
			weight = 1 // Ensure minimum weight of 1
		}
		weights[wmember.String()] = weight

		// Create multiple copies of the member based on its weight
		for i := 0; i < weight; i++ {
			expandedMembers = append(expandedMembers, &weightedMemberWrapper{
				member: wmember,
				suffix: i,
			})
		}
	}

	// Create the base consistent hash ring with expanded members
	baseConsistent := New(expandedMembers, config)

	return &WeightedWrapper{
		Consistent: baseConsistent,
		weights:    weights,
	}
}

// weightedMemberWrapper wraps a WeightedMember to create multiple virtual nodes
type weightedMemberWrapper struct {
	member WeightedMember
	suffix int
}

func (w *weightedMemberWrapper) String() string {
	return fmt.Sprintf("%s#%d", w.member.String(), w.suffix)
}

// AddWeighted adds a new weighted member to the consistent hash circle.
func (w *WeightedWrapper) AddWeighted(member WeightedMember) {
	weight := member.Weight()
	if weight <= 0 {
		weight = 1
	}

	// Check if member already exists
	if _, exists := w.weights[member.String()]; exists {
		return
	}

	w.weights[member.String()] = weight

	// Add multiple copies based on weight
	for i := 0; i < weight; i++ {
		virtualMember := &weightedMemberWrapper{
			member: member,
			suffix: i,
		}
		w.Consistent.Add(virtualMember)
	}
}

// RemoveWeighted removes a weighted member from the consistent hash circle.
func (w *WeightedWrapper) RemoveWeighted(name string) {
	weight, exists := w.weights[name]
	if !exists {
		return
	}

	// Remove all virtual nodes for this member
	for i := 0; i < weight; i++ {
		virtualName := fmt.Sprintf("%s#%d", name, i)
		w.Consistent.Remove(virtualName)
	}

	delete(w.weights, name)
}

// LocateKeyWeighted finds a home for given key and returns the original weighted member
func (w *WeightedWrapper) LocateKeyWeighted(key []byte) WeightedMember {
	virtualMember := w.Consistent.LocateKey(key)
	if virtualMember == nil {
		return nil
	}

	// Extract the original member from the virtual wrapper
	if wrapper, ok := virtualMember.(*weightedMemberWrapper); ok {
		return wrapper.member
	}

	return nil
}

// GetWeightedMembers returns a list of original weighted members (without duplicates)
func (w *WeightedWrapper) GetWeightedMembers() []WeightedMember {
	var result []WeightedMember
	seen := make(map[string]bool)

	allMembers := w.Consistent.GetMembers()
	for _, member := range allMembers {
		if wrapper, ok := member.(*weightedMemberWrapper); ok {
			memberName := wrapper.member.String()
			if !seen[memberName] {
				seen[memberName] = true
				result = append(result, wrapper.member)
			}
		}
	}

	return result
}

// GetWeights returns a copy of the weight distribution
func (w *WeightedWrapper) GetWeights() map[string]int {
	result := make(map[string]int)
	for name, weight := range w.weights {
		result[name] = weight
	}
	return result
}

// GetClosestNWeighted returns the closest N weighted members to a key
func (w *WeightedWrapper) GetClosestNWeighted(key []byte, count int) ([]WeightedMember, error) {
	if count <= 0 {
		return []WeightedMember{}, nil
	}

	// Check if we have enough unique members
	if count > len(w.weights) {
		return nil, ErrInsufficientMemberCount
	}

	var result []WeightedMember
	seen := make(map[string]bool)

	// Calculate a reasonable multiplier based on the maximum weight
	maxWeight := 1
	for _, weight := range w.weights {
		if weight > maxWeight {
			maxWeight = weight
		}
	}

	// Start with a reasonable estimate: count * maxWeight
	// This ensures we get enough virtual members to find all unique members
	requestCount := count * maxWeight

	for {
		virtualMembers, err := w.Consistent.GetClosestN(key, requestCount)
		if err != nil {
			return nil, err
		}

		result = result[:0]   // Reset result slice
		for k := range seen { // Clear seen map
			delete(seen, k)
		}

		for _, virtualMember := range virtualMembers {
			if len(result) >= count {
				break
			}

			if wrapper, ok := virtualMember.(*weightedMemberWrapper); ok {
				memberName := wrapper.member.String()
				if !seen[memberName] {
					seen[memberName] = true
					result = append(result, wrapper.member)
				}
			}
		}

		// If we got enough unique members, we're done
		if len(result) >= count {
			return result[:count], nil
		}

		// If we've requested all available virtual members but still don't have enough unique ones
		if requestCount >= len(w.Consistent.GetMembers()) {
			if len(result) == 0 {
				return nil, ErrInsufficientMemberCount
			}
			return result, nil
		}

		// Double the request count and try again
		requestCount *= 2
		if requestCount > len(w.Consistent.GetMembers()) {
			requestCount = len(w.Consistent.GetMembers())
		}
	}
}
