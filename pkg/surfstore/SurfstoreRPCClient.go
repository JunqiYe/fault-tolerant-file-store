package surfstore

import (
	context "context"
	"fmt"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	log.Println("PutBlock: putting", block.BlockSize)
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		log.Println("PutBlock:", err.Error())
		return err
	}
	*succ = success.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	// if err != nil {
	// 	return err
	// }
	// // Here this method comes from generate code by proto
	// c := NewBlockStoreClient(conn)

	// // perform the call
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()

	// var err2 error
	// in := &blockHashesIn
	// blockHashesOut, err2 = c.HasBlocks(ctx, in) // todo

	// if err2 != nil {
	// 	log.Println("Error in client getFileInfoMap")
	// 	conn.Close()
	// 	return err2
	// }
	// // serverFileInfoMap = &infoMap.FileInfoMap

	// // close the connection
	// return conn.Close()
	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		// Here this method comes from generate code by proto
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		infoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		if err != nil {
			// log.Panic("Error in client getFileInfoMap", err.Error())
			conn.Close()
			continue
			// return err
		}

		*serverFileInfoMap = infoMap.FileInfoMap

		// close the connection
		return conn.Close()
	}
	log.Fatal("getFileInfoMap error")
	return fmt.Errorf("GetFileInfoMap error")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		// Here this method comes from generate code by proto
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		ver, err := c.UpdateFile(ctx, fileMetaData)

		if err != nil {
			// log.Panic("Error in client UpdateFile", err.Error())
			conn.Close()
			continue
			// return err
		}

		latestVersion = &ver.Version

		// close the connection
		return conn.Close()
	}
	log.Fatal("UpdateFile error")
	return fmt.Errorf("UpdateFile error")
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	// Here this method comes from generate code by proto
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	bh, err := c.GetBlockHashes(ctx, &emptypb.Empty{})

	if err != nil {
		log.Panic("Error in client UpdateFile", err.Error())
		conn.Close()
		return err
	}

	*blockHashes = bh.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		// Here this method comes from generate code by proto
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		bh := BlockHashes{Hashes: make([]string, len(blockHashesIn))}
		copy(bh.Hashes, blockHashesIn)
		BlockMap, err := c.GetBlockStoreMap(ctx, &bh)

		// fmt.Printf("Key: %v, Val: %v \n", key, val.Val)

		if err != nil {
			// log.Panic("Error in client GetBlockStoreAddr")
			conn.Close()
			continue
			// return err
		}

		for blockAddr, blockHash := range BlockMap.BlockStoreMap {
			for _, hash := range blockHash.Hashes {
				(*blockStoreMap)[hash] = blockAddr

			}
		}
		// close the connection
		return conn.Close()
	}
	log.Fatal("GetBlockStoreMap error")
	return fmt.Errorf("GetBlockStoreMap error")

}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		// Here this method comes from generate code by proto
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		blockAddrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})

		// fmt.Printf("Key: %v, Val: %v \n", key, val.Val)
		log.Print("[Client GetBlockStoreAddrs]:", blockAddrs.BlockStoreAddrs)
		if err != nil {
			// log.Panic("Error in client GetBlockStoreAddr")
			conn.Close()
			continue
			// return err
		}
		*blockStoreAddrs = blockAddrs.BlockStoreAddrs

		// close the connection
		return conn.Close()
	}

	log.Fatal("GetBlockStoreAddrs error")
	return fmt.Errorf("GetBlockStoreAddrs error")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
