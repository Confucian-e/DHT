package main

import (
	"crypto/sha1"
	"fmt"
	"math/rand"
)

var BucketSize = 3
var IDLength = 20
var BucketsNum = 160
var PeersNum = 5
var PeersNewNum = 200

type Node struct {
	ID string
}

type Bucket struct {
	Nodes []Node
}

type K_Bucket struct {
	Buckets   []Bucket
	KeyValues []KeyValue
}

type Peer struct {
	KBucket *K_Bucket
	ID      string
}

func generateRandomID() string {
	id := ""
	for i := 0; i < IDLength; i++ {
		id += string(rand.Intn(10) + '0')
	}
	return id
}

func NewK_Bucket() *K_Bucket {
	kb := &K_Bucket{}
	for i := 0; i < BucketsNum; i++ {
		kb.Buckets = append(kb.Buckets, Bucket{})
	}
	return kb
}

func (kb *K_Bucket) findBucketIndex(nodeID string) int {
	for i, b := range kb.Buckets {
		if len(b.Nodes) < BucketSize || commonPrefixLength(b.Nodes[0].ID, nodeID) > i {
			return i
		}
	}
	return BucketsNum - 1
}

func (kb *K_Bucket) insertNode(nodeID string) {
	bucketIndex := kb.findBucketIndex(nodeID)
	bucket := &kb.Buckets[bucketIndex]

	if len(bucket.Nodes) >= BucketSize {
		if commonPrefixLength(bucket.Nodes[0].ID, nodeID) > bucketIndex {
			newBucket := Bucket{}
			newBucket.Nodes = append(newBucket.Nodes, bucket.Nodes[BucketSize/2:]...)
			bucket.Nodes = bucket.Nodes[:BucketSize/2]
			kb.Buckets = append(kb.Buckets, Bucket{})
			copy(kb.Buckets[bucketIndex+1:], kb.Buckets[bucketIndex:])
			kb.Buckets[bucketIndex] = *bucket
			kb.Buckets[bucketIndex+1] = newBucket
			bucket = &kb.Buckets[bucketIndex]
		} else {
			// 直接丢弃新节点
			return
		}
	}

	bucket.Nodes = append(bucket.Nodes, Node{ID: nodeID})
}

func (kb *K_Bucket) printBucketContents() {
	for i, b := range kb.Buckets {
		fmt.Printf("Bucket %d: ", i)
		for _, node := range b.Nodes {
			fmt.Printf("%s ", node.ID)
		}
		fmt.Println()
	}
}

func commonPrefixLength(str1, str2 string) int {
	commonLen := 0
	for i := 0; i < len(str1) && i < len(str2); i++ {
		if str1[i] == str2[i] {
			commonLen++
		} else {
			break
		}
	}
	return commonLen
}

func (p *Peer) FindNode(nodeID string) bool {
	p.KBucket.insertNode(nodeID)
	bucketIndex := p.KBucket.findBucketIndex(nodeID)
	bucket := p.KBucket.Buckets[bucketIndex]
	if len(bucket.Nodes) > 0 && bucket.Nodes[0].ID == nodeID {
		return true
	}
	if len(bucket.Nodes) >= 2 {
		selectedNodes := make(map[int]bool)
		for len(selectedNodes) < 2 {
			randIndex := rand.Intn(len(bucket.Nodes))
			selectedNodes[randIndex] = true
			fmt.Printf("Send FindNode request to node: %s\n", bucket.Nodes[randIndex].ID)
		}
	}
	return false
}

type KeyValue struct {
	Key   string
	Value []byte
}

func (p *Peer) SetValue(key string, value []byte) bool {
	if key != fmt.Sprintf("%x", sha1.Sum(value)) {
		return false
	}

	for _, kv := range p.KBucket.KeyValues {
		if kv.Key == key {
			return true
		}
	}

	p.KBucket.KeyValues = append(p.KBucket.KeyValues, KeyValue{Key: key, Value: value})

	bucketIndex := p.KBucket.findBucketIndex(key)
	bucket := p.KBucket.Buckets[bucketIndex]

	if len(bucket.Nodes) < 2 {
		return true
	}
	nodesToSend := bucket.Nodes[:2]

	for _, node := range nodesToSend {
		fmt.Printf("Send SetValue request to node: %s\n", node.ID)
	}

	return true
}

func (p *Peer) GetValue(key string) []byte {
	for _, kv := range p.KBucket.KeyValues {
		if kv.Key == key {
			return kv.Value
		}
	}

	bucketIndex := p.KBucket.findBucketIndex(key)
	bucket := p.KBucket.Buckets[bucketIndex]

	if len(bucket.Nodes) < 2 {
		return nil
	}

	nodesToSend := bucket.Nodes[:2]

	for _, node := range nodesToSend {
		fmt.Printf("Send GetValue request to node: %s\n", node.ID)
	}

	return nil
}

func main() {
	peers := make([]*Peer, PeersNum)
	for i := 0; i < PeersNum; i++ {
		peerID := generateRandomID()
		peers[i] = &Peer{NewK_Bucket(), peerID}
		fmt.Printf("Init node %d with ID: %s\n", i, peerID)
	}

	keys := make([]string, PeersNewNum)
	for i := 0; i < PeersNewNum; i++ {
		value := make([]byte, 20)
		_, err := rand.Read(value)
		if err != nil {
			panic(err)
		}
		key := fmt.Sprintf("%x", sha1.Sum(value))
		keys[i] = key
		peerIndex := rand.Intn(PeersNum)
		peers[peerIndex].SetValue(key, value)
		fmt.Printf("Set key %s on node %d\n", key, peerIndex)
	}

	for i := 0; i < 100; i++ {
		keyIndex := rand.Intn(PeersNewNum)
		key := keys[keyIndex]
		peerIndex := rand.Intn(PeersNum)
		value := peers[peerIndex].GetValue(key)
		if value != nil {
			fmt.Printf("Get value for key %s from node %d\n", key, peerIndex)
		} else {
			fmt.Printf("No value found for key %s on node %d\n", key, peerIndex)
		}
	}
}