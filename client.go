package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
// import "log"
// import "fmt"
// import "time"
// import "sync"


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	//reqTs map[int64]time.Time
	// reqStatus map[int64]bool
	// mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	//ck.reqTs = make(map[int64]time.Time)
	// ck.reqStatus = make(map[int64]bool)
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	reqId := nrand()
	args:= GetArgs{Key: key, ReqId: reqId}
	reply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}
	}
	return ""
}

func (ck *Clerk) makeCallAndWait(rpc string, args *GetArgs, reply *GetReply) string {
	// if ck.reqTs[reqId].IsZero() {
	// 	ck.reqTs[reqId] = time.Now()
	// }
	// timer := time.NewTimer(10*time.Second)
	for {
		ok := ck.server.Call(rpc, args, reply)
		if ok {
			return reply.Value
		}
	}
	return ""
}
// func (ck *Clerk) makeCall(ch chan string, rpc string, args *GetArgs, reply *GetReply) {
// 	ok := ck.server.Call(rpc, args, reply)
// 	if !ok {
// 		fmt.Printf("Get RPC failed.\n")
// 		return
// 	} else {
// 		ch <- reply.Value
// 	}
// }

// func (ck *Clerk) makeCallAndWaitPut(ch chan string, rpc string, args *PutAppendArgs, reply *PutAppendReply) string {
// 	// if ck.reqTs[reqId].IsZero() {
// 	// 	ck.reqTs[reqId] = time.Now()
// 	// }
// 	// timer := time.NewTimer(10*time.Second)
// 	for {
// 		ok := ck.server.Call(rpc, args, reply)
// 		if ok {
// 			return reply.Value
// 		}
// 	}
// 	return ""
// }
// func (ck *Clerk) makeCallPut(ch chan string, rpc string, args *PutAppendArgs, reply *PutAppendReply) {
// 	ck.mu.Lock()
// 	defer ck.mu.Unlock()
// 	if ck.reqStatus[args.ReqId] {
// 		return
// 	}
// 	ok := ck.server.Call(rpc, args, reply)
// 	if !ok {
// 		fmt.Printf("Get RPC failed.\n")
// 		return
// 	} else {
// 		ch <- reply.Value
// 	}
// 	ck.reqStatus[args.ReqId] = true
// }

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, ReqId: nrand()}
	reply := PutAppendReply{}
	// if op == "Put" {
	// 	ok := ck.server.Call("KVServer.Put", &args, &reply)
	// 	if !ok {
	// 		fmt.Printf("Put RPC failed.\n")
	// 		return ""
	// 	}
	// } else {
	// 	ok := ck.server.Call("KVServer.Append", &args, &reply)
	// 	if !ok {
	// 		fmt.Printf("Append RPC failed.\n")
	// 		return ""
	// 	}
	// }
	// ch := make(chan string)
	rpc:= "KVServer."+op
	for {
		ok := ck.server.Call(rpc, &args, &reply)
		if ok {
			// log.Printf("rpc: %v w/ args: %v returned %v", rpc, args, reply)
			return reply.Value
		}
	}
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
