package gobloom

import (
	"math"
	"sync"

	"github.com/willf/bloom"
)

type Filter struct {
	mtx    sync.Mutex
	filter *bloom.BloomFilter
}

// NewFilter returns a new bloom filter.
// nitems is the number of items that will pass through the filter.
// p is the probability of a false positive.
func NewFilter(nitems int, p float64) *Filter {
	var f Filter

	// Taken from https://hur.st/bloomfilter/
	mem := math.Ceil(float64(nitems) * math.Log(p) / math.Log(1/math.Pow(2, math.Ln2)))
	k := math.Round((mem / float64(nitems)) * math.Ln2)
	f.filter = bloom.New(uint(mem), uint(k))

	return &f
}

// TestAndAdd returns false if the key has not been seen before, and
// true if it is likely that it has seen the key before. This operation
// will add the key to the bloom filter.
func (f *Filter) TestAndAdd(key []byte) bool {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.filter.TestAndAdd(key)
}
