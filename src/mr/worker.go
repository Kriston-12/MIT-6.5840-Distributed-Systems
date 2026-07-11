package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var (
	coordSockName = "" // socket for coordinator
	debugLogging  = false
)

func debugf(format string, args ...interface{}) {
	if debugLogging {
		log.Printf(format, args...)
	}
}

func reduceHelper(reducef func(string, []string) string, reduceTaskId int, kva []KeyValue) {
	// oname := fmt.Sprintf("mr-out-%d", reduceTaskId)
	tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-reduce-%d-*", reduceTaskId))
	if err != nil {
		log.Fatalf("cannot create temp file for reduce task %d", reduceTaskId)
	}
	writer := bufio.NewWriter(tempFile)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		// The loop below finds the first index j such that kva[j].Key != kva[i].Key
		// which means range[i, j) contains the same key
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		// Loop through the range [i, j) to collect all values for the same key
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		if _, err := fmt.Fprintf(writer, "%v %v\n", kva[i].Key, output); err != nil {
			log.Fatalf("cannot write reduce task %d output: %v", reduceTaskId, err)
		}

		i = j
	}
	if err := writer.Flush(); err != nil {
		log.Fatalf("cannot flush reduce task %d output: %v", reduceTaskId, err)
	}
	if err := tempFile.Close(); err != nil {
		log.Fatalf("cannot close reduce task %d output: %v", reduceTaskId, err)
	}
	if err := os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", reduceTaskId)); err != nil {
		log.Fatalf("cannot publish reduce task %d output: %v", reduceTaskId, err)
	}
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	args := RPCArgs{}
	reply := RPCReply{}

	for {
		reply = RPCReply{}
		ok := call("Coordinator.Assign", &args, &reply)
		if !ok {
			log.Fatalf("call Coordinator.Assign failed")
			return
		}

		taskType := reply.TaskType
		if taskType == "map" || taskType == "reduce" {
			debugf("RECV worker=%d type=%s task=%d nReduce=%d", os.Getpid(), taskType, reply.TaskId, reply.NReduce)
		}
		switch taskType {
		case "map":
			kva := []KeyValue{}
			inputFile := reply.MapFile
			contentBytes, err := os.ReadFile(inputFile)
			if err != nil {
				log.Fatalf("cannot read %v", inputFile)
				return
			}
			content := string(contentBytes)
			mapStarted := time.Now()
			kva = mapf(inputFile, content)
			mapElapsed := time.Since(mapStarted)
			debugf("MAPF function elapsed=%s", mapElapsed)
			// Create intermediate files for each reduce task
			fileWriteStarted := time.Now()
			intermediateFiles := make([]*os.File, reply.NReduce)
			writers := make([]*bufio.Writer, reply.NReduce)
			encoders := make([]*json.Encoder, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				intermediateFiles[i], err = os.CreateTemp(".", fmt.Sprintf("mr-map-%d-*", i))
				if err != nil {
					log.Fatalf("cannot create intermediate file for map task %d: %v", reply.TaskId, err)
				}
				writers[i] = bufio.NewWriter(intermediateFiles[i])
				encoders[i] = json.NewEncoder(writers[i])
			}
			for _, kv := range kva {
				reduceTaskNum := ihash(kv.Key) % reply.NReduce
				encoders[reduceTaskNum].Encode(&kv)
			}
			for i := 0; i < reply.NReduce; i++ {
				// 我之前在担心如果worker超时了，那么这个map task会被多次执行
				// 那么可能会有多个形如mr-taskId-reduceTaskId的中间文件存在，导致reduce阶段重复计算
				// 但是rename会覆盖同名文件，所以不需要担心这个问题，在reduce stage的时候只有一个唯一的intermediate文件
				if err := writers[i].Flush(); err != nil {
					log.Fatalf("cannot flush intermediate file for map task %d: %v", reply.TaskId, err)
				}
				if err := intermediateFiles[i].Close(); err != nil {
					log.Fatalf("cannot close intermediate file for map task %d: %v", reply.TaskId, err)
				}
				if err := os.Rename(intermediateFiles[i].Name(), fmt.Sprintf("mr-%d-%d", reply.TaskId, i)); err != nil {
					log.Fatalf("cannot publish intermediate file for map task %d: %v", reply.TaskId, err)
				}
			}
			fileWriteElapsed := time.Since(fileWriteStarted)
			debugf("MAPF file write elapsed=%s", fileWriteElapsed)
			debugf("SEND worker=%d type=map task=%d", os.Getpid(), reply.TaskId)
			ok := call("Coordinator.ReportTaskCompletion", &DoneArgs{TaskType: "map", TaskId: reply.TaskId, Attempt: reply.Attempt}, &RPCReply{})
			if !ok {
				log.Fatalf("call ReportTaskCompletion failed")
				return
			}

		case "reduce":
			// fmt.Printf("Worker %d received a reduce task with id %d and files %v\n", os.Getpid(), reply.taskId, reply.files)
			// reply.taskId is the unique identifier of intermediate files. Any intermediate file alike pattern "mr-*-reply.taskId" will be used for reduce task.
			pattern := fmt.Sprintf("mr-*-%d", reply.TaskId)
			intermediateFiles, err := filepath.Glob(pattern)
			if err != nil {
				log.Fatalf("cannot find intermediate files for pattern %v", pattern)
				return
			}
			kva := []KeyValue{}
			for _, intermediateFile := range intermediateFiles {
				file, err := os.Open(intermediateFile)
				if err != nil {
					log.Fatalf("cannot open %v", intermediateFile)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(kva))
			reduceHelper(reducef, reply.TaskId, kva)
			debugf("SEND worker=%d type=reduce task=%d", os.Getpid(), reply.TaskId)
			ok := call("Coordinator.ReportTaskCompletion", &DoneArgs{TaskType: "reduce", TaskId: reply.TaskId, Attempt: reply.Attempt}, &RPCReply{})
			if !ok {
				log.Fatalf("call ReportTaskCompletion failed")
				return
			}
		case "wait":
			time.Sleep(1 * time.Second)
		case "exit":
			debugf("EXIT worker=%d", os.Getpid())
			return
		default:
			log.Fatalf("Unknown task type: %v", taskType)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	log.Printf("%d: call %q failed: %v", os.Getpid(), rpcname, err)

	return false
}
