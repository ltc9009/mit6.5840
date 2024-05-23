package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"
import "errors"
import "time"
import "io"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// copied from mrsequential.go
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample()
	pid := os.Getpid()


	for {
		err := CallRequestTask(mapf, reducef, pid)
		if err != nil {
			log.Fatalf("cannot request task %v", err)
			break
		}
		//time.Sleep(time.Second)
	}

}
func handleMapJob(mapf func(string, string) []KeyValue, job MapReduceJob, NReduce int) error {
	// fmt.Printf("start handle map job for file %v\n", job.Filename[0])
	filename := job.Filename[0]
	// pid := job.Pid
	index := job.Index

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	intermediate := make([][]KeyValue, NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % NReduce
		intermediate[r] = append(intermediate[r], kv)
	}
	output := make([]string, NReduce)
	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", index, r)
		ofile, _ := ioutil.TempFile("", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
		output[r] = oname
	}

	/*file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kvar := mapf(filename, string(content))
	sort.Sort(ByKey(kvar))
	intermediate := make([]string, NReduce)
	tmpfiles := []*os.File{}
	for i := 0; i < NReduce; i++ {
		tmpfile,err := ioutil.TempFile("/tmp", "mr-in-"+strconv.Itoa(i))
		if err != nil {
			log.Fatalf("cannot create temp file for %v reducer.", i)
			return err
		}
		tmpfiles = append(tmpfiles, tmpfile)
	}
	for _, kv := range kvar {
		hash := ihash(kv.Key)
		reduce_index := hash % NReduce
		enc := json.NewEncoder(tmpfiles[reduce_index])
		err:=enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode json. %v", err)
			return err
		}
	}
	for i := 0; i < NReduce; i++ {
		new_name := "mr-" + strconv.Itoa(pid)+ "-" + strconv.Itoa(index) + "-" + strconv.Itoa(i)
		old_name := tmpfiles[i].Name()
		os.Rename(old_name, new_name)
		// fmt.Printf("rename map output file %v to %v\n", old_name, new_name)
		intermediate[i] = new_name
	}
	// fmt.Printf("report map task with output %v", intermediate)
	*/
	return CallReportTask(job, output)
}
func handleReduceJob(reducef func(string, []string) string, job MapReduceJob) error {
	fmt.Printf("start handle reduce job %v\n", job.Filename)
	kva := []KeyValue{}
	for _, filename := range job.Filename {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("reducer job cannot open input file: %v", filename)
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
			  break
			}
			// fmt.Printf("json decode got %v %v", kv.Key, kv.Value)
			kva = append(kva, kv)
		  }
		  file.Close()
	}
	sort.Sort(ByKey(kva))
	tmpfile, err := ioutil.TempFile("/tmp", "mr-out-"+strconv.Itoa(job.Pid))
	if err != nil {
		log.Fatalf("cannot create temp file for reduce %v", job.Pid)
		return err
	}
	i := 0
	for i < len(kva) {
		j := i+1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output:=reducef(kva[i].Key, values)
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	new_output := "mr-out-" + strconv.Itoa(job.Index)
	os.Rename(tmpfile.Name(), new_output)
	return CallReportTask(job, []string{new_output})
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
func CallRequestTask(mapf func(string, string) []KeyValue,
reducef func(string, []string) string, pid int) error {
	args := RequestTaskArgs{Pid: pid}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		fmt.Printf("call request task failed")
		return errors.New("call Coordinator.RequestTask failed")
	}
	log.Printf("request task got reply : %v", reply)
	if reply.ExistJob {
		fmt.Println("no more task to execute.")
		os.Exit(0)
	}
	if reply.Job.Type == MapJob {
		return handleMapJob(mapf, reply.Job, reply.NReduce)
	}
	if reply.Job.Type == ReduceJob {
		return handleReduceJob(reducef, reply.Job)
	}
	if reply.Job.Type == WaitJob {
		time.Sleep(time.Second)
		return nil
	}

	return errors.New("unknown task type")
}

func CallReportTask(job MapReduceJob, ofiles []string) error {
	args := ReportTaskArgs{Job:job, OutputFiles: ofiles}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		fmt.Printf("call report task failed!\n")
		return errors.New("call report task failed")
	}
	return nil
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
