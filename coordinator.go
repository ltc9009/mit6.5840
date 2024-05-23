package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"
// import "strconv"
import "github.com/gammazero/deque"
import "sync/atomic"

type Coordinator struct {
	// Your definitions here.
	// Maps task index to MapJob
	MapJobs map[int]JobStatus
	// Queue of MapReduceJob to be assigned to workers
	MapperQueue deque.Deque[MapReduceJob]
	// Maps task index to ReduceJob
	ReduceJobs map[int]JobStatus
	// Queue of MapReduceJob to be assigned to workers
	ReducerQueue deque.Deque[MapReduceJob]

	MapPending atomic.Int32
	ReducePending atomic.Int32
	NReduce int
	NMap int
	Files []string
	// Intermediate is a 2D array, each row is a list of words from a file
	// Intermediate[i][j] is the output of jth mapper job for ith reducer
	Intermediate [][]string
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.NReduce = c.NReduce
	if c.MapperQueue.Len() > 0 {
		reply.Job = c.MapperQueue.PopFront()
		reply.Job.Start_ts = time.Now()
		reply.Job.Status = Assigned
		c.MapJobs[reply.Job.Index] = Assigned
		c.MapPending.Add(1)

		timer := time.NewTimer(10*time.Second)
		go func() {
			fmt.Printf("timer filed for mapper job %v with pid %v\n", reply.Job.Index, args.Pid)
			<-timer.C
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.MapJobs[reply.Job.Index] != Finished  {
				c.MapJobs[reply.Job.Index] = Unassigned
				c.MapperQueue.PushBack(reply.Job)
				c.MapPending.Add(-1)
			}
		}()
		return nil
	} else if c.MapPending.Load() > 0 {
		reply.Job = MapReduceJob{Type: WaitJob}
		return nil
	}
	if c.ReducerQueue.Len() > 0 {
		reply.Job = c.ReducerQueue.PopFront()
		reply.Job.Start_ts = time.Now()
		reply.Job.Status = Assigned
		reply.Job.Filename = c.Intermediate[reply.Job.Index]
		fmt.Printf("assign reducer job %v with intermediate filesnames %v\n", reply.Job.Index, c.Intermediate[reply.Job.Index])
		c.ReduceJobs[reply.Job.Index] = Assigned
		c.ReducePending.Add(1)


		timer := time.NewTimer(10*time.Second)
		go func() {
			<-timer.C
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.ReduceJobs[reply.Job.Index] != Finished  {
				c.ReduceJobs[reply.Job.Index] = Unassigned

				c.ReducerQueue.PushBack(reply.Job)
				c.ReducePending.Add(-1)
			}
		}()
		return nil
	} else if c.ReducePending.Load() > 0 {
		reply.Job = MapReduceJob{Type: WaitJob}
		return nil
	}
	reply.ExistJob = true
	return nil
	

}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Job.Type == MapJob {
		if c.MapJobs[args.Job.Index] == Assigned {
			c.MapJobs[args.Job.Index] = Finished
			c.MapPending.Add(-1)
			for i := 0; i < c.NReduce; i++ {
				c.Intermediate[i] = append(c.Intermediate[i], args.OutputFiles[i])
			}

		}
	} else if args.Job.Type == ReduceJob {
		if c.ReduceJobs[args.Job.Index] == Assigned {
			c.ReduceJobs[args.Job.Index] = Finished
			c.ReducePending.Add(-1)
		}
	}
	return nil

	// c.mu.Lock()
	/*defer c.mu.Unlock()
	if args.Job.Type == MapJob {
		if args.Job.Start_ts.Add(time.Second * 10).Before(time.Now()) {
			fmt.Println("Map job ", args.Job.Pid, " finished later than 10 seconds")
			return nil
		}
		for i := 0; i < c.NReduce; i++ {
			c.Intermediate[i] = append(c.Intermediate[i], args.OutputFiles[i])
		}
		c.MapJobs[args.Job.Index] = Finished
		// fmt.Printf("c.Intermediate before: %v\n", c.Intermediate)
		fmt.Printf("mark map job %v finished\n", args.Job.Index)
		fmt.Printf("c.Intermediate updated to: %v\n", c.Intermediate)
		// fmt.Printf("from coordinator: %v\n", c.MapJobs[args.Job.Index])
	} else if args.Job.Type == ReduceJob {
		if args.Job.Start_ts.Add(time.Second * 10).Before(time.Now()) {
			fmt.Println("Reduce job ", args.Job.Pid, " finished later than 10 seconds")
			return nil
		}
		fmt.Printf("mark reduce job %v finished by %v \n", args.Job.Index, args.Job.Pid)
		new_name := "mr-out-" + strconv.Itoa(args.Job.Index)
		fmt.Println("Reduce job with output: %v finished, write to output: %v", args.OutputFiles[0], new_name)
		os.Rename(args.OutputFiles[0], new_name)
		c.ReduceJobs[args.Job.Index] = Finished
	}*/
}
//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	fmt.Printf("checking if all reduce tasks finished, mapper queue size:%v, mapperpending: %v, queue size: %v, pending: %v\n", c.MapperQueue.Len(), c.MapPending.Load(), c.ReducerQueue.Len(), c.ReducePending.Load())
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.
	if c.ReducerQueue.Len() == 0 && c.ReducePending.Load() == 0 {
		fmt.Println("all reduce tasks finished")
		return true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.Files = files
	c.Intermediate = make([][]string, c.NReduce)
	c.NMap = len(files)
	c.MapJobs = make(map[int]JobStatus)
	for i := 0; i < c.NMap; i++ {
		// c.MapJobs[i] = Unassigned
		c.MapperQueue.PushBack(MapReduceJob{Index: i, Status: Unassigned, Filename: []string{c.Files[i]}, Type: MapJob})
	}
	c.ReduceJobs = make(map[int]JobStatus)
	for i := 0; i < c.NReduce; i++ {
		c.ReducerQueue.PushBack(MapReduceJob{Index: i, Status: Unassigned, Filename: []string{}, Type: ReduceJob})
	}
	c.server()
	return &c
}
