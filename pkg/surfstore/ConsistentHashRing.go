package surfstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string // server hash to server addr
}

// true if hash1 smaller than hash2
func isSmallerThan(hash1 string, hash2 string) bool {
	key1, err1 := hex.DecodeString(hash1)
	key2, err2 := hex.DecodeString(hash2)
	if err1 != nil {
		panic(err1)
	}
	if err2 != nil {
		panic(err2)
	}

	for ptr := 0; ptr < len(key1); ptr++ {
		if bytes.Compare(key1, key2) == -1 {
			return true
		} else if bytes.Compare(key1, key2) == 1 {
			return false
		}
	}
	return false
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	hashes := make([]string, 0)
	for serverHash := range c.ServerMap {
		hashes = append(hashes, serverHash)
	}

	// sort file
	sort.Slice(hashes, func(i, j int) bool {
		return isSmallerThan(hashes[i], hashes[j])
	})
	var rst string
	log.Println("GetResbonsibleServer, ", blockId, hashes)
	ptr := len(hashes) - 1
	for ptr >= 0 {
		if isSmallerThan(blockId, hashes[ptr]) {
			if ptr == 0 {
				rst = hashes[ptr]
				log.Println("responsibleServer: ptr==0", rst)
				return c.ServerMap[rst]
			}
			ptr -= 1
		} else {
			if ptr == len(hashes)-1 {
				rst = hashes[0]
				log.Println("responsibleServer: ptr == len-1", rst)
				return c.ServerMap[rst]
			}
			break
		}
	}

	rst = hashes[ptr+1]
	log.Println("responsibleServer: ", rst)
	return c.ServerMap[rst]

}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	chr := ConsistentHashRing{ServerMap: make(map[string]string)}

	for _, addr := range serverAddrs {
		serverHash := chr.Hash("blockstore" + addr)
		chr.ServerMap[serverHash] = addr
	}

	log.Println("NewConsistentHashRing", chr.ServerMap)
	return &chr
}
