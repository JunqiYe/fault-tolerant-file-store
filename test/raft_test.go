package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"log"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	term := int64(1)
	var leader bool

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, nil, nil, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	term = int64(2)
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, nil, nil, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	t.Log("leader1 gets a request")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	log.Println("heartbeat 1")
	log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	log.Println("heartbeat 2")
	// one final call to sendheartbeat (from spec)
	log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

// leader1 gets a request when the majority of the cluster is crashed.
func Test1(t *testing.T) {
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 2 to be the leader
	// TEST
	leaderIdx := 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// here node 2 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	// update a file on node 2
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	// log.Println("heartbeat 1")
	// log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// log.Println("heartbeat 2")
	// // one final call to sendheartbeat (from spec)
	// log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

}

// leader1 gets a request. leader1 gets another request.
func Test2(t *testing.T) {
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 2 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// here node 2 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	// test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	// test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	// update a file on node 2
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta1ver2 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       2,
		BlockHashList: nil,
	}
	// filemeta2 := &surfstore.FileMetaData{
	// 	Filename:      "testfile2",
	// 	Version:       1,
	// 	BlockHashList: nil,
	// }

	log.Println("\n\n===========client0 file1 syncs ==================")
	rst, err := test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	log.Println("[Test] updateFileResult1: ", rst, err)

	log.Println(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	log.Println("\n\n===========client0 file2 syncs ==================")
	rst, err = test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1ver2)
	log.Println("[Test] updateFileResult2: ", rst, err)

	log.Println("\n\n=========== midpoint check ==================")
	test.Clients[leaderIdx].GetInternalState(test.Context, &emptypb.Empty{})
	test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})

	log.Println(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// log.Println("=========== rst ==================")
	test.Clients[leaderIdx].GetInternalState(test.Context, &emptypb.Empty{})
	test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})
}

// block when majority down
func Test3(t *testing.T) {
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 2 to be the leader
	// TEST
	leaderIdx := 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// here node 2 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	// update a file on node 2
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}

	log.Println("===========client1 syncs ==================")
	rst, err := test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	log.Println("[Test] updateFileResult1: ", rst, err)
}

// isLeader:true  term:1
// log:{term:1  fileMetaData:{filename:"testfile1"  version:1}}
// log:{term:1  fileMetaData:{filename:"testfile2"  version:1}}
// metaMap:{
// 	fileInfoMap:{key:"testfile1"  value:{filename:"testfile1"  version:1}}
// 	fileInfoMap:{key:"testfile2"  value:{filename:"testfile2"  version:1}}
// } <nil>

// leader1 gets a request while the majority of the cluster is down. leader1 crashes. the other nodes come back. leader2 is elected
func Test4(t *testing.T) {
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 2 to be the leader
	// TEST
	leaderIdx := 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// here node 2 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	// update a file on node 2
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}

	log.Println("===========client1 syncs ==================")
	rst, err := test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	log.Println("[Test] updateFileResult1: ", rst, err)
	log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	log.Println("===========client1 crash, restore others ==================")
	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	log.Println("===========client2 leader ==================")
	test.Clients[2].SetLeader(test.Context, &emptypb.Empty{})
	log.Print(test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{}))

	log.Println("===========client internal ==================")
	for i := range test.Clients {
		state, _ := test.Clients[i].GetInternalState(test.Context, &emptypb.Empty{})
		log.Println("client ", i, state)
	}
	// rst, err = test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
	// log.Println("[Test] updateFileResult2: ", rst, err)
	// log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// log.Println("===========client0 restore ==================")
	// // test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	// log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

	// log.Println("===========client2 restore ==================")
	// // test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	// log.Print(test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{}))

}

// isLeader:true  term:1
// log:{term:1  fileMetaData:{filename:"testfile1"  version:1}}
// log:{term:1  fileMetaData:{filename:"testfile2"  version:1}}
// metaMap:{
// 	fileInfoMap:{key:"testfile1"  value:{filename:"testfile1"  version:1}}
// 	fileInfoMap:{key:"testfile2"  value:{filename:"testfile2"  version:1}}
// } <nil>

func TestRaftNewLeaderPushesUpdates(t *testing.T) {
	t.Log("leader1 gets a request while the majority of the cluster is down. leader1 crashes. the other nodes come back. leader2 is elected")
	cfgPath := "./config_files/5nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	leaderIdx := 0
	fileMeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	test.Clients[3].Crash(test.Context, &emptypb.Empty{})

	go test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta1)
	// test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	time.Sleep(100 * time.Millisecond)

	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[3].Restore(test.Context, &emptypb.Empty{})

	leaderIdx = 4
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{Term: 1, FileMetaData: fileMeta1})
	for idx, server := range test.Clients {
		var term int64
		if idx == 0 {
			term = 1
		} else {
			term = 2
		}
		_, err := CheckInternalState(nil, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state of server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftLogsConsistent(t *testing.T) {
	t.Log("leader1 gets a request while a minority of the cluster is down. leader1 crashes. the other crashed nodes are restored. leader2 gets a request. leader1 is restored.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	leaderIdx := 0
	fileMeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}
	fileMeta2 := &surfstore.FileMetaData{
		Filename:      "testfile2",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	version, err := test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta1)
	if err != nil || version.Version != 1 {
		t.FailNow()
	}

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	v2, err := test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta2)
	if err != nil || v2.Version != 1 {
		t.FailNow()
	}

	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[fileMeta1.Filename] = fileMeta1
	goldenMeta[fileMeta2.Filename] = fileMeta2
	goldenLog := make([]*surfstore.UpdateOperation, 0)

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: fileMeta1,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: fileMeta2,
	})

	term := int64(2)
	for idx, server := range test.Clients {
		_, err := CheckInternalState(nil, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func Test10(t *testing.T) {
	// leader1 gets several requests while all other nodes are crashed. leader1 crashes. all other nodes are restored. leader2 gets a request. leader1 is restored.
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testfile2",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta3 := &surfstore.FileMetaData{
		Filename:      "testfile3",
		Version:       1,
		BlockHashList: nil,
	}
	// set node 2 to be the leader
	// TEST
	// leaderIdx := 0
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// here node 2 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	// update a file on node 2

	go test.Clients[0].UpdateFile(test.Context, filemeta1)
	time.Sleep(100 * time.Millisecond)

	go test.Clients[0].UpdateFile(test.Context, filemeta2)
	time.Sleep(100 * time.Millisecond)

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	log.Print(test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{}))

	go test.Clients[1].UpdateFile(test.Context, filemeta3)

	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	log.Print(test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{}))

	test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
}
