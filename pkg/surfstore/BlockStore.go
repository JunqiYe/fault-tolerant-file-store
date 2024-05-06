package surfstore

import (
	context "context"
	"errors"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	log.Println("blockstore server: GetBlock got data")
	hash := blockHash.Hash
	blockData, ok := bs.BlockMap[hash]
	if !ok {
		return nil, errors.New("")
	}
	log.Println(len(blockData.BlockData))
	return blockData, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hashString := GetBlockHashString(block.BlockData)

	bs.BlockMap[hashString] = &Block{BlockData: block.BlockData, BlockSize: block.BlockSize}
	log.Println("blockstore server: putblock ", hashString, block.BlockSize)
	rst := Success{}
	rst.Flag = true
	return &rst, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	panic("todo")
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	bh := BlockHashes{Hashes: make([]string, 0)}

	for blockHash := range bs.BlockMap {
		bh.Hashes = append(bh.Hashes, blockHash)
	}

	return &bh, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
