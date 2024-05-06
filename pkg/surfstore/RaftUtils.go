package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		id:             id,
		peers:          make([]string, len(config.RaftAddrs)),
		commitIndex:    -1,
		lastApplied:    -1,
		matchedIndex:   -1,
		pendingIndex:   -1,
		pendingCommits: make([]*chan bool, 0),
		term:           0,
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
	}

	copy(server.peers, config.RaftAddrs)

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)

	listen, err := net.Listen("tcp", server.peers[server.id])
	if err != nil {
		log.Panic(err.Error())
	}
	log.Println("Start Raft on hostAddr", server.peers[server.id])
	return grpcServer.Serve(listen)
}
