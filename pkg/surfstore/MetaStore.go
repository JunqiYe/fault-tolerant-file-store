package surfstore

import (
	context "context"
	"errors"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	dataMap := FileInfoMap{FileInfoMap: m.FileMetaMap}
	// dataMap.FileInfoMap = make(map[string]*FileMetaData)
	// dataMap.FileInfoMap["somefile.txt"] = &FileMetaData{Filename: "somefile.txt", Version: 2}
	// dataMap.FileInfoMap = m.FileMetaMap
	// for filename, metaData := range m.FileMetaMap {
	// 	dataMap.FileInfoMap[filename] = metaData
	// }
	log.Println("metastore: getFileInfoMap send data", dataMap.FileInfoMap)
	return &dataMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	serverFileMeta, ok := m.FileMetaMap[fileMetaData.Filename]
	defer PrintMetaMap(m.FileMetaMap)

	if !ok {
		log.Println("metaStore: new file", fileMetaData.Filename, "ver: ", fileMetaData.Version)
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil

	} else {
		log.Printf("update file version---- %s serverMeta version: %d, request file ver %d\n", fileMetaData.Filename, serverFileMeta.Version, fileMetaData.Version)

		if fileMetaData.Version <= serverFileMeta.Version {
			return &Version{Version: -1}, errors.New("bad version")
		}

		serverFileMeta.Version = fileMetaData.Version
		serverFileMeta.BlockHashList = fileMetaData.BlockHashList
		return &Version{Version: serverFileMeta.Version}, nil

	}

	// updateTarget_fileName := fileMetaData.Filename
	// updateTarget_ver := fileMetaData.Version

	// var serverMeta *FileMetaData
	// var ok bool
	// serverMeta, ok = m.FileMetaMap[updateTarget_fileName]

	// if !ok {
	// 	m.FileMetaMap[updateTarget_fileName] = fileMetaData
	// 	serverMeta = fileMetaData
	// 	log.Println("metaStore: updateFile no file")
	// } else if serverMeta.Version > updateTarget_ver {
	// 	return &Version{Version: -1}, errors.New("bad version")
	// }

}

// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	log.Println("metastore: GetBlockStoreAddr send data")
// 	blockData := BlockStoreAddr{}
// 	blockData.Addr = m.BlockStoreAddr

// 	return &blockData, nil
// }

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	rst := BlockStoreMap{BlockStoreMap: make(map[string]*BlockHashes)}
	for _, hash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(hash)
		_, ok := rst.BlockStoreMap[server]
		if !ok {
			rst.BlockStoreMap[server] = &BlockHashes{Hashes: make([]string, 0)}
		}
		rst.BlockStoreMap[server].Hashes = append(rst.BlockStoreMap[server].Hashes, hash)
	}
	log.Println("GetBlockStoreMap", rst.BlockStoreMap)
	return &rst, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	blockAddrs := BlockStoreAddrs{BlockStoreAddrs: make([]string, len(m.BlockStoreAddrs))}
	copy(blockAddrs.BlockStoreAddrs, m.BlockStoreAddrs)

	log.Println("metastore: GetBlockStoreAddr send data", blockAddrs.BlockStoreAddrs)
	return &blockAddrs, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
