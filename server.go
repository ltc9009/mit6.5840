package kvsrv

import (
	"log"
	"sync"
	"strings"
)


const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	db map[string][]string
	reqStatus map[int64]bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value =strings.Join(kv.db[args.Key], "")
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// fmt.Printf("Put: %v\n", args.Key)
	if kv.reqStatus[args.ReqId] {
		reply.Value = strings.Join(kv.db[args.Key], "")
		return
	}
	kv.db[args.Key] = []string{args.Value}
	reply.Value = args.Value
	kv.reqStatus[args.ReqId] = true
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.reqStatus[args.ReqId] {
		last := len(kv.db[args.Key]) - 1
		reply.Value = strings.Join(kv.db[args.Key][:last], "")
		return
	}
	reply.Value = strings.Join(kv.db[args.Key], "")
	kv.db[args.Key] = append(kv.db[args.Key], args.Value)
	kv.reqStatus[args.ReqId] = true
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.db = make(map[string][]string)
	// You may need initialization code here.
	kv.reqStatus = make(map[int64]bool)

	return kv
}
