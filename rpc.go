package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "fmt"
import "time"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}


// Add your RPC definitions here.

type JobStatus int

const (
	Unassigned JobStatus = iota
	Assigned
	Finished
)

type JobType int
const (
	MapJob JobType = iota
	ReduceJob
	WaitJob
)
type MapReduceJob struct {
	Index int
	Status JobStatus
	Start_ts time.Time
	Filename []string
	Pid int
	Type JobType
}

type RequestTaskArgs struct {
  Pid int
}

type RequestTaskReply struct {
	NReduce int
	Job MapReduceJob
	// Signal worker to exsit since all tasks are done.
	ExistJob bool
}

type ReportTaskArgs struct {
	Job MapReduceJob
	OutputFiles []string
}

type ReportTaskReply struct {

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	fmt.Println(s)
	return s
}
