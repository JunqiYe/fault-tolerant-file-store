package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func min(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	id           int64
	peers        []string
	commitIndex  int64
	matchedIndex int64
	pendingIndex int64
	// Added for discussion
	pendingCommits []*chan bool
	lastApplied    int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isCrashedMutex.Lock()
	crashed := s.isCrashed
	s.isCrashedMutex.Unlock()

	if crashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	leader := s.isLeader
	s.isLeaderMutex.Unlock()
	if !leader {
		return nil, ERR_NOT_LEADER
	}

	// is leader,
	// if a majority of the nodes are working, should return the correct answer;
	// if a majority of the nodes are crashed, should block until a majority recover.
	for {
		majority, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			log.Panic("GetFileInfoMap", err.Error())
		}
		if majority.Flag {
			return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	s.isCrashedMutex.Lock()
	crashed := s.isCrashed
	s.isCrashedMutex.Unlock()

	if crashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	leader := s.isLeader
	s.isLeaderMutex.Unlock()
	if !leader {
		return nil, ERR_NOT_LEADER
	}

	// is leader,
	// if a majority of the nodes are working, should return the correct answer;
	// if a majority of the nodes are crashed, should block until a majority recover.
	for {
		majority, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			log.Panic("GetFileInfoMap", err.Error())
		}
		if majority.Flag {
			return s.metaStore.GetBlockStoreMap(ctx, blockHashesIn)
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	s.isCrashedMutex.Lock()
	crashed := s.isCrashed
	s.isCrashedMutex.Unlock()

	if crashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	leader := s.isLeader
	s.isLeaderMutex.Unlock()
	if !leader {
		return nil, ERR_NOT_LEADER
	}

	// is leader,
	// if a majority of the nodes are working, should return the correct answer;
	// if a majority of the nodes are crashed, should block until a majority recover.
	for {
		majority, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			log.Panic("GetFileInfoMap", err.Error())
		}
		if majority.Flag {
			return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		}
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	log.Println("[RaftServer]: UpdateFile", s.id, fileMetaData.Filename, fileMetaData.Version)
	fmt.Println("[RaftServer]: UpdateFile", s.id, fileMetaData.Filename, fileMetaData.Version)

	s.isCrashedMutex.Lock()
	crashed := s.isCrashed
	s.isCrashedMutex.Unlock()

	if crashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	leader := s.isLeader
	s.isLeaderMutex.Unlock()
	if !leader {
		return nil, ERR_NOT_LEADER
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term: s.term,
		FileMetaData: &FileMetaData{
			Filename:      fileMetaData.Filename,
			Version:       fileMetaData.Version,
			BlockHashList: make([]string, len(fileMetaData.BlockHashList)),
		},
	})
	copy(s.log[len(s.log)-1].FileMetaData.BlockHashList, fileMetaData.BlockHashList)
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	// s.lastApplied += 1
	s.pendingIndex += 1
	log.Println("[RaftServer]UpdateFile pendingcommits ", len(s.pendingCommits))
	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		log.Printf("[RaftServer %d] UpdateFile commit %s", s.id, fileMetaData.Filename)
		temp := FileMetaData{
			Filename:      fileMetaData.Filename,
			Version:       fileMetaData.Version,
			BlockHashList: make([]string, len(fileMetaData.BlockHashList)),
		}
		copy(temp.BlockHashList, fileMetaData.BlockHashList)
		return s.metaStore.UpdateFile(ctx, &temp)
	} else {
		// log.Fatal("majority offline, failed to commit file")
		// log.Printf("[RaftServer %d] UpdateFile failed to commit %s", s.id, fileMetaData.Filename)
	}
	return nil, fmt.Errorf("failed to commit file")

	log.Printf("[RaftServer %d] UpdateFile failed to commit %s", s.id, fileMetaData.Filename)
	return nil, fmt.Errorf("failed to commit file")
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	log.Println("[RaftServer]: spawning sendToAllFollowersInParallel")
	// send entry to all my followers and count the replies

	totalResponses := 1
	totalAppends := 1
	for {
		s.isCrashedMutex.Lock()
		crashed := s.isCrashed
		s.isCrashedMutex.Unlock()

		if crashed {
			fmt.Println("crashed exiting")
			*s.pendingCommits[s.pendingIndex] <- false
			return
		}

		responses := make(chan bool, len(s.peers)-1)
		// contact all the follower, send some AppendEntries call
		for idx, addr := range s.peers {
			if int64(idx) == s.id {
				continue
			}
			// input := AppendEntryInput{
			// 	Term:         s.term,
			// 	PrevLogTerm:  -1,
			// 	PrevLogIndex: s.lastApplied,
			// 	Entries:      s.log,
			// 	LeaderCommit: s.commitIndex,
			// }

			if idx == 1 {
				// fmt.Printf("[AppendEntries] \n [term:%d\nprevLogIDX%d\nentries %s\n leadercommitidx %d]\n", input.Term, input.PrevLogIndex, input.Entries, input.LeaderCommit)
			}
			go s.sendToFollower(ctx, addr, responses)
		}

		if crashed {
			fmt.Println("crashed exiting")
			*s.pendingCommits[s.pendingIndex] <- false
			return
		}
		totalResponses = 1
		totalAppends = 1

		// wait in loop for responses
		for {
			s.isCrashedMutex.Lock()
			crashed := s.isCrashed
			s.isCrashedMutex.Unlock()
			if crashed {
				fmt.Println("crashed exiting")
				*s.pendingCommits[s.pendingIndex] <- false
				return
			}
			result := <-responses
			totalResponses++
			if result {
				totalAppends++
			}
			if totalResponses == len(s.peers) {
				break
			}
		}

		log.Println("got all response, num appends", totalAppends)

		if totalAppends > len(s.peers)/2 {
			s.commitIndex += 1
			break
		} else {
			// log.Println("[sendToAllFollowersInParallel] majority crashed, retrying...")
			// break
		}
		time.Sleep(150 * time.Millisecond)
		s.isCrashedMutex.Lock()
		crashed = s.isCrashed
		s.isCrashedMutex.Unlock()
		if crashed {
			fmt.Println("crashed exiting")
			*s.pendingCommits[s.pendingIndex] <- false
			return
		}
	}

	log.Println("commit index:", s.commitIndex, "size pending commit", len(s.pendingCommits))
	*s.pendingCommits[s.pendingIndex] <- totalAppends > len(s.peers)/2

}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, responses chan bool) {
	s.isCrashedMutex.Lock()
	crashed := s.isCrashed
	s.isCrashedMutex.Unlock()

	if crashed {
		responses <- false
		return
	}

	prevTerm := int64(-1)
	if s.lastApplied != -1 {
		prevTerm = s.log[s.lastApplied].Term
	}
	input := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  prevTerm,
		PrevLogIndex: s.lastApplied,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	// TODO check all errors
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Panic("[sendToFollower]:", err.Error())
		responses <- false
		return
	}
	client := NewRaftSurfstoreClient(conn)

	out, err := client.AppendEntries(ctx, &input)
	if err != nil {
		// log.Panic("[sendToFollower]:", err.Error())
		conn.Close()
		responses <- false
		return
	}

	// TODO check output
	// TODO here matchedIndex
	responses <- out.Success
}

func isSameEntry(entry1 *UpdateOperation, entry2 *UpdateOperation) bool {
	// log.Println("[isSameEntry]", entry1.Term, entry2.Term, entry1.Term == entry2.Term,
	// 	"\n", entry1.FileMetaData.Filename == entry2.FileMetaData.Filename, "\n",
	// 	entry1.FileMetaData.Version == entry2.FileMetaData.Version)

	return entry1.Term == entry2.Term &&
		entry1.FileMetaData.Filename == entry2.FileMetaData.Filename &&
		entry1.FileMetaData.Version == entry2.FileMetaData.Version
}

// =========================================================
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, leader *AppendEntryInput) (*AppendEntryOutput, error) {
	s.isCrashedMutex.Lock()
	crashed := s.isCrashed
	s.isCrashedMutex.Unlock()

	if crashed {
		log.Println("[RaftServer]: crashed AppendEntries: serverid-", s.id)
		return nil, ERR_SERVER_CRASHED
	}

	log.Println("[RaftServer]: process AppendEntries: serverid-", s.id, "leader:{", leader, "}")
	// if s.id == 1 {
	// 	fmt.Printf("[AppendEntries 1 own data] \n [term:%d\nentries %s]\n", s.term, s.log)
	// }
	if leader.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		s.term = leader.Term
	}

	out := AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      true,
		MatchedIndex: -1,
	}

	// 1.
	if leader.Term < s.term {
		out.Success = false
	}

	// 2.
	if leader.PrevLogIndex != -1 {
		if int64(len(s.log)) > leader.PrevLogIndex && s.log[leader.PrevLogIndex].Term != leader.PrevLogTerm {
			out.Success = false
		}
	}
	log.Printf("[RaftServer %d] AppendEntries copy log bb4 %s\n", s.id, s.log)

	// 3.
	lastIdx := -1
	for idx, entry := range s.log {
		if idx < len(leader.Entries) && !isSameEntry(entry, leader.Entries[idx]) {
			s.log = s.log[:idx]
			break
		}
		lastIdx = idx
	}
	log.Printf("[RaftServer %d] AppendEntries copy log b4 %s\n", s.id, s.log)
	// if s.id == 1 {
	// 	fmt.Printf("[RaftServer %d] AppendEntries copy log before step4: %s\n", s.id, s.log)
	// }

	// 4.
	if lastIdx+1 < len(leader.Entries) {
		for _, entry := range leader.Entries[lastIdx+1:] {
			temp2 := *(entry.FileMetaData)
			temp := UpdateOperation{Term: entry.Term, FileMetaData: &temp2}
			s.log = append(s.log, &temp)
		}
	}
	log.Printf("[RaftServer %d] AppendEntries copy log after %s\n", s.id, s.log)
	// if s.id == 1 && len(s.log) == 2 {

	// 	fmt.Printf("[RaftServer %d] AppendEntries copy log after step4: %s\n", s.id, s.log)
	// 	fmt.Printf("\n\n%x, %x\n\n", &(s.log[0]), &(s.log[1]))
	// }

	// 5.
	log.Printf("[RaftServer %d] AppendEntries leader.LeaderCommit %d | s.commitIndex%d", s.id, leader.LeaderCommit, s.commitIndex)
	if leader.LeaderCommit > s.commitIndex {
		s.commitIndex = min(leader.LeaderCommit, int64(len(s.log)-1)) // ??
	}

	for s.lastApplied < s.commitIndex {
		log.Printf("[RaftServer %d] last applied %d / %d commit index", s.id, s.lastApplied, s.commitIndex)
		s.lastApplied++
		tempFileMetaData := FileMetaData{
			Filename:      s.log[s.lastApplied].FileMetaData.Filename,
			Version:       s.log[s.lastApplied].FileMetaData.Version,
			BlockHashList: make([]string, len(s.log[s.lastApplied].FileMetaData.BlockHashList)),
		}
		// entry := s.log[s.lastApplied]
		copy(tempFileMetaData.BlockHashList, s.log[s.lastApplied].FileMetaData.BlockHashList)
		s.metaStore.UpdateFile(ctx, &tempFileMetaData)
	}
	out.MatchedIndex = s.lastApplied
	log.Println("[RaftServer]: result AppendEntries: serverid-", s.id, "server result:{", "succ", out.Success, "term", out.Term, "matchidx", out.MatchedIndex, "}")
	// if s.id == 1 && len(s.log) == 2 {
	// 	fmt.Printf("[RaftServer %d] AppendEntries at the end: %s\n", s.id, s.log)
	// 	fmt.Printf("\n\n%x, %x\n\n", &(s.log[0]), &(s.log[1]))
	// }
	return &out, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Println("[RaftServer]: SetLeader", s.id)
	fmt.Println("[RaftServer]: SetLeader", s.id)

	s.isCrashedMutex.Lock()
	crashed := s.isCrashed
	s.isCrashedMutex.Unlock()

	if crashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	s.term += 1

	return &Success{Flag: true}, nil
}

// return true if majority is true
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Println("[RaftServer]: SendHeartbeat", s.id)
	fmt.Println("[RaftServer]: SendHeartbeat", s.id)

	s.isCrashedMutex.Lock()
	crashed := s.isCrashed
	s.isCrashedMutex.Unlock()

	if crashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	leader := s.isLeader
	s.isLeaderMutex.Unlock()
	if !leader {
		return nil, ERR_NOT_LEADER
	}

	cnt := 1
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		// Here this method comes from generate code by proto
		c := NewRaftSurfstoreClient(conn)

		// // perform the call
		// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		// defer cancel()

		prevTerm := int64(-1)
		if s.lastApplied != -1 {
			prevTerm = s.log[s.lastApplied].Term
		}
		in := AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: s.lastApplied,
			PrevLogTerm:  prevTerm,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		// if idx == 1 {
		// 	fmt.Printf("[AppendEntries] \n [term:%d\nprevLogIDX%d\nentries %s\n leadercommitidx %d]\n", in.Term, in.PrevLogIndex, in.Entries, in.LeaderCommit)
		// }
		out, err := c.AppendEntries(ctx, &in)
		if err != nil {
			log.Println("[SendHeartbeat]:", err.Error())
			conn.Close()
			continue
			// return nil, err
		}

		// close the connection
		if out.Success {
			cnt++
		}
		// flag.Flag = flag.Flag && out.Success
		conn.Close()
	}

	return &Success{Flag: cnt > len(s.peers)/2}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	fmt.Println("[Crash ", s.id, "]")
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	fmt.Println("[Restore ", s.id, "]")
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()
	fmt.Println("[GetInternalState ", s.id, "] :", state.Log, state.MetaMap.state)
	// something := FileInfoMap{}
	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
