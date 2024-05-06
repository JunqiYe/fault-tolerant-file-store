package surfstore

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

const createTable string = `CREATE TABLE IF NOT EXISTS indexes(
	fileName TEXT,
  	version INT,
  	hashIndex INT,
  	hashValue TEXT
);`

func check(e error) {
	if e != nil {
		log.Panic(e)
	}
}

func createLocalDB(baseDir string) *sql.DB {
	indexdbFilename := filepath.Join(".", baseDir, DEFAULT_META_FILENAME)
	database, err := sql.Open("sqlite3", indexdbFilename)
	if err != nil {
		fmt.Println(err.Error())
	}

	// create table in .db file
	statement, err := database.Prepare(createTable)
	if err != nil {
		fmt.Println(err.Error())
	}
	statement.Exec()

	return database
}

const insertHash string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`
const deleteFile string = `delete from indexes where fileName == ?;`

func updateDB(db *sql.DB, metaData *FileMetaData, pulledFromRemote bool) {
	// query filename
	// update version
	// update hash list
	if metaData.Version != 0 {
		statement, err := db.Prepare(deleteFile)
		if err != nil {
			log.Panic(err.Error())
		}
		statement.Exec(metaData.Filename)
	}

	if !pulledFromRemote {
		metaData.Version += 1
	}

	statement, err := db.Prepare(insertHash)
	if err != nil {
		log.Panic(err.Error())
	}
	for hashIndex, shasum := range metaData.BlockHashList {
		statement.Exec(metaData.Filename, metaData.Version, hashIndex, shasum)
	}

}

const getFileQuery string = `select version, hashIndex, hashValue from indexes where fileName == ? order by hashIndex asc;`

func getMetaFromDB(db *sql.DB, filename string, metaData *FileMetaData) {
	log.Println("querying ", filename)
	rows, err := db.Query(getFileQuery, filename)
	if err != nil {
		log.Panic(err.Error())
	}
	hashList := make([]string, 0)
	var version int = 0
	var hashIndex int = 0
	var hashValue string = ""
	for rows.Next() {
		rows.Scan(&version, &hashIndex, &hashValue)
		log.Println("version", version, "index", hashIndex, hashValue)
		hashList = append(hashList, hashValue)
	}
	metaData.Filename = filename
	metaData.Version = int32(version)
	metaData.BlockHashList = hashList
	// return hashList, version
}

const getAllFilename string = `select distinct fileName from indexes;`

func getFilenameFromDB(db *sql.DB) []string {
	rows, err := db.Query(getAllFilename)
	if err != nil {
		log.Panic(err.Error())
	}

	indexFilenames := make([]string, 0)
	var filename string = ""
	for rows.Next() {
		rows.Scan(&filename)
		indexFilenames = append(indexFilenames, filename)
	}
	return indexFilenames
}

func getIndexFileInfo(db *sql.DB, indexFileInfo *FileInfoMap) {
	indexFilenames := getFilenameFromDB(db)
	for _, filename := range indexFilenames {
		temp := FileMetaData{}
		getMetaFromDB(db, filename, &temp)
		indexFileInfo.FileInfoMap[filename] = &temp
	}
}

func computeHashListAndDataBlock(filename string, blockSize int) ([]string, map[string][]byte) {
	hashList := make([]string, 0)
	hash2blockMap := make(map[string][]byte)
	blockbuff := make([]byte, blockSize)

	f, err := os.Open(filename)
	check(err)
	defer f.Close()

	log.Println("checking file", filename)
	index := 0
	for {
		read, err := f.Read(blockbuff)
		dataRead := make([]byte, read)
		copy(dataRead, blockbuff)
		if err != nil && err == io.EOF {
			log.Println("reached EOF")
			break
		} else {
			check(err)
		}

		hash := GetBlockHashString(dataRead)
		index += 1
		hashList = append(hashList, hash)
		hash2blockMap[hash] = dataRead
	}

	return hashList, hash2blockMap
}

func isSameHash(serverList []string, clientCopy []string) bool {
	if len(serverList) != len(clientCopy) {
		return false
	}

	for i := range clientCopy {
		if serverList[i] != clientCopy[i] {
			return false
		}
	}
	return true
}

func processFileDir(db *sql.DB, fileInfo *FileInfoMap, currDir string, blockSize int) {
	dir := currDir
	log.Println(dir)

	files, err := ioutil.ReadDir(filepath.Join(".", currDir))
	if err != nil {
		log.Fatal(err)
	}

	// reads all files in the directory
	for _, file := range files {
		// fmt.Println(file.Name(), file.IsDir())
		if !file.IsDir() {
			if file.Name() == DEFAULT_META_FILENAME {
				continue
			}

			// get clientHashList from db
			// _, version := getMetaFromDB(db, file.Name())
			clientHashList, _ := computeHashListAndDataBlock(filepath.Join(".", currDir, file.Name()), blockSize)

			currFileMetaData := FileMetaData{Filename: file.Name(), Version: int32(0), BlockHashList: clientHashList}
			fileInfo.FileInfoMap[currFileMetaData.Filename] = &currFileMetaData
		}
	}
}

// func (client RPCClient) putClientFiles(blockStorePort string, clientFile2metaMap *FileInfoMap) {

// 	// put all client files
// 	for filename := range clientFile2metaMap.FileInfoMap {
// 		serverMetadata := clientFile2metaMap.FileInfoMap[filename]

// 		err := client.UpdateFile(serverMetadata, &serverMetadata.Version)
// 		if err != nil {
// 			log.Panic("client: failed to update file metadata")
// 		}

// 		log.Println("Client putting file", filename)
// 		_, hash2blockMap := computeHashListAndDataBlock(filepath.Join(".", client.BaseDir, filename), client.BlockSize)

// 		for hash := range hash2blockMap {
// 			block := hash2blockMap[hash]
// 			size := int32(len(block))
// 			datablock := &Block{BlockData: block, BlockSize: size}

// 			var succ bool
// 			log.Println("client putting block:", hash)
// 			client.PutBlock(datablock, blockStorePort, &succ)
// 			if !succ {
// 				log.Panic("client failed to put block")
// 			}
// 		}
// 	}
// }

func (client RPCClient) putClientFileBlock(metaData *FileMetaData) error {

	// put one client files
	log.Println("Client putting file", metaData.Filename)
	blockHashList, hash2blockMap := computeHashListAndDataBlock(filepath.Join(".", client.BaseDir, metaData.Filename), client.BlockSize)

	hash2BlockStore := make(map[string]string)
	client.GetBlockStoreMap(blockHashList, &hash2BlockStore)

	for hash := range hash2blockMap {
		block := hash2blockMap[hash]
		size := int32(len(block))
		datablock := &Block{BlockData: block, BlockSize: size}

		var succ bool
		log.Println("client putting block:", hash)
		client.PutBlock(datablock, hash2BlockStore[hash], &succ)
		if !succ {
			log.Panic("client failed to put block")
		}
	}
	return nil
}

func (client RPCClient) putClientFileMeta(metaData *FileMetaData) error {
	log.Println("putClientFile: ", metaData.Filename, metaData.Version)
	err := client.UpdateFile(metaData, &metaData.Version)
	if err != nil {
		log.Print("client: failed to update file metadata")
		return err
	}
	return nil
}

func (client RPCClient) getFromServerAndAssemble(serverFileMap *FileMetaData) {
	// get hash

	blockHashList := serverFileMap.BlockHashList
	fileName := serverFileMap.Filename
	if blockHashList[0] == "0" {
		os.Remove(filepath.Join(client.BaseDir, fileName))
		// check(e)
		return
	}
	f, err := os.Create(filepath.Join(".", client.BaseDir, fileName))
	check(err)

	defer f.Close()

	// addr to hash
	hash2BlockStore := make(map[string]string)
	client.GetBlockStoreMap(blockHashList, &hash2BlockStore)

	for _, hash := range blockHashList {
		block := Block{}
		client.GetBlock(hash, hash2BlockStore[hash], &block)
		log.Println("client: got from server bytes", len(block.BlockData))
		// d2 := []byte{115, 111, 109, 101, 10}
		n2, err := f.Write(block.BlockData)
		check(err)
		log.Printf("client: wrote %d bytes\n", n2)

	}

	// write to file using hash lookup
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// check local
	// pull remote
	// update local

	log.Println("creating database")
	db := createLocalDB(client.BaseDir)

	// get blockstore address
	// var blockStorePort string
	// err := client.GetBlockStoreAddrs(&blockStorePort) // TODO
	// check(err)
	// log.Println("client got block store address", blockStorePort)

	localFileInfo := FileInfoMap{FileInfoMap: make(map[string]*FileMetaData)}
	indexFileInfo := FileInfoMap{FileInfoMap: make(map[string]*FileMetaData)}
	// get fileinfo from metastore, index db, and local file system
	processFileDir(db, &localFileInfo, client.BaseDir, client.BlockSize)
	getIndexFileInfo(db, &indexFileInfo)

	// // compare clientHashList
	// sameHash := isSameHash(index2hash_idb, clientHashList)
	// log.Println("client sameHash:", sameHash)
	// if !sameHash {
	// 	updateDB(db, version+1, file.Name(), clientHashList)
	// }

	filesToSync := make(map[string]bool)
	// compare local and index files
	log.Println("\ncompare local and index files\n")
	for localFileName, localFileMeta := range localFileInfo.FileInfoMap {
		indexFileMeta, ok := indexFileInfo.FileInfoMap[localFileName]
		if !ok {
			// new file, not in index db -> add to index db
			log.Println("new file", localFileName, "------------meta version", localFileMeta.Version)
			updateDB(db, localFileMeta, false)
			// client.putClientFile(blockStorePort, localFileMeta) // TODO maybe move this ?
			filesToSync[localFileName] = true
		} else {
			// not new file, check if content is the same, update hash
			localFileMeta.Version = indexFileMeta.Version

			log.Println("not new file", localFileName)
			sameHash := isSameHash(localFileMeta.BlockHashList, indexFileMeta.BlockHashList)
			log.Println("client sameHash:", sameHash)
			if !sameHash {
				updateDB(db, localFileMeta, false)

				// getMetaFromDB(db, localFileName, localFileMeta)
				filesToSync[localFileName] = true
			}
			// client.putClientFile(blockStorePort, localFileMeta) // TODO maybe move this ?
		}
	}

	// deleted File
	log.Println("\nupdate deleted file\n")
	for indexFileName, indexFileMeta := range indexFileInfo.FileInfoMap {
		// localMetaData := localFileInfo.FileInfoMap[localFileName]
		_, ok := localFileInfo.FileInfoMap[indexFileName]
		if !ok && indexFileMeta.BlockHashList[0] != "0" {
			// file previously in index, later removed
			// indexFileMeta.BlockHashList = make([]string, 1)
			for i := range indexFileMeta.BlockHashList {
				indexFileMeta.BlockHashList[i] = "0"
			}
			updateDB(db, indexFileMeta, false)
			log.Println("client file was deleted: ", indexFileName)
			filesToSync[indexFileName] = false
			// client.putClientFile(blockStorePort, indexFileMeta) // TODO maybe move this ?
		}
	}

	// update index file info
	// get remote, compare remote and index
	log.Println("\nget remote, compare remote and index\n")

	indexFileInfo = FileInfoMap{FileInfoMap: make(map[string]*FileMetaData)}
	getIndexFileInfo(db, &indexFileInfo)

	serverFileInfo := FileInfoMap{}
	client.GetFileInfoMap(&serverFileInfo.FileInfoMap)

	for remoteFileName, remoteFileMeta := range serverFileInfo.FileInfoMap {
		indexFileMeta, ok := indexFileInfo.FileInfoMap[remoteFileName]
		if !ok {
			//remote file not present in index, pull data
			updateDB(db, remoteFileMeta, true)
			client.getFromServerAndAssemble(remoteFileMeta)
		} else {
			// conflict check version
			if remoteFileMeta.Version >= indexFileMeta.Version {
				client.getFromServerAndAssemble(remoteFileMeta)
				updateDB(db, remoteFileMeta, true)
				// remove from filestosync if exists
				_, ok := filesToSync[remoteFileName]
				if ok {
					delete(filesToSync, remoteFileName)
				}
			}

			// // file present in index, check hash, if different, pull data
			// sameHash := isSameHash(indexFileMeta.BlockHashList, remoteFileMeta.BlockHashList)
			// log.Println("compare remote and index hash for ", remoteFileName, "same hash? ", sameHash)
			// if !sameHash {
			// 	updateDB(db, remoteFileMeta, true)
			// 	client.getFromServerAndAssemble(remoteFileMeta)
			// }
		}

		// client.putClientFile(blockStorePort, remoteFileMeta) // TODO maybe move this ?
	}

	log.Println("putting all files to sync\n")
	log.Print("file to sync list", filesToSync)
	for filename, deleted := range filesToSync {
		// send blcok
		temp := FileMetaData{}
		getMetaFromDB(db, filename, &temp)
		// indexFileInfo.FileInfoMap[filename] = &temp
		if deleted {
			err := client.putClientFileBlock(&temp)
			check(err)
		}

		err2 := client.putClientFileMeta(&temp)
		if err2 != nil {
			// version conflict, discard local
			serverFileInfo := FileInfoMap{}
			client.GetFileInfoMap(&serverFileInfo.FileInfoMap)
			client.getFromServerAndAssemble(serverFileInfo.FileInfoMap[filename])
			updateDB(db, serverFileInfo.FileInfoMap[filename], true)
		}
	}

}
