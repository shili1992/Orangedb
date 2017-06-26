package Orangedb

import (
	"strconv"
	"sort"
	"fmt"
)


// Chooser maps keys to shards
type Chooser interface {
	// SetBuckets sets the list of known buckets from which the chooser should select
	SetBuckets([]string) error
	// Choose returns a bucket for a given key
	Choose(key string) int
	// Buckets returns the list of known buckets
	Buckets() []string
}


// Shard is a named storage backend
type Shard struct {
	Name    string
	//Backend Storage
}


type HashChooser struct {
	mod  int
}

func NewHashChooser(mod int) *HashChooser {
	c := &HashChooser{
		mod: mod,
	}
	return c
}

func (c *HashChooser) SetBuckets(buckets []string) error {
	return nil
}

func (c *HashChooser) Choose(key string) int {
	value ,_:= strconv.Atoi(key)
	return value % c.mod;
}

func (c *HashChooser) ChooseReplicas(key string, n int) []string { return nil }

func (c *HashChooser) Buckets() []string { return nil }







type RangeChooser struct {
	rangeList []string
	partitionNum  int
}

func NewRangeChooser() *RangeChooser {
	c := &RangeChooser{
	}
	return c
}

func (c *RangeChooser) SetBuckets(buckets []string) error {
	c.rangeList = buckets
	sort.Strings(c.rangeList)
	fmt.Println(c.rangeList)
	c.partitionNum = len(buckets)+1
	return nil
}

func (c *RangeChooser) Choose(key string) int {
	var index int = 0
	for index = 0; index < len(c.rangeList); index++ {
		if key < c.rangeList[index] {
			break
		}
	}
	return index
}

func (c *RangeChooser) ChooseReplicas(key string, n int) []string { return nil }

func (c *RangeChooser) Buckets() []string { return nil }