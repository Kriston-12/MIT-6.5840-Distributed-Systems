package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "path/filepath"
import "encoding/json"
import "sort"
import "time"

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

var coordSockName string // socket for coordinator

func reduceHelper(reducef func(string, []string) string, reduceTaskId int, kva []KeyValue) {
	// oname := fmt.Sprintf("mr-out-%d", reduceTaskId)
	tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-reduce-%d-*", reduceTaskId))
	if err != nil {
		log.Fatalf("cannot create temp file for reduce task %d", reduceTaskId)
	}

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
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", reduceTaskId))
	tempFile.Close()
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	args := RPCArgs{}
	reply := RPCReply{}

	for {
		ok := call("Worker.askForTask", &args, &reply)
		if !ok {
			log.Fatalf("call askForTask failed")
		}

		taskType := reply.taskType
		switch taskType {
			case "map":
				fmt.Printf("Worker %d received a map task with id %d and files %v\n", os.Getpid(), reply.taskId, reply.files)
				kva := []KeyValue{}
				// For each worker of map task, it will receive only one input file
				inputFile := reply.files[0]
				contentBytes, err := os.ReadFile(inputFile)
				if err != nil {
					log.Fatalf("cannot read %v", inputFile)
				}
				content := string(contentBytes)
				kva = mapf(inputFile, content)
				// Create intermediate files for each reduce task
				intermediateFiles := make([]*os.File, reply.nReduce)
				encoders := make([]*json.Encoder, reply.nReduce)
				for i := 0; i < reply.nReduce; i++ {
					intermediateFiles[i], _ = os.CreateTemp("",fmt.Sprintf("mr-map-%d-*", i))
					encoders[i] = json.NewEncoder(intermediateFiles[i])
				}
				for _, kv := range kva {
					reduceTaskNum := ihash(kv.Key) % reply.nReduce
					encoders[reduceTaskNum].Encode(&kv)
				}
				for i := 0; i < reply.nReduce; i++ {
					// 我之前在担心如果worker超时了，那么这个map task会被多次执行
					// 那么可能会有多个形如mr-taskId-reduceTaskId的中间文件存在，导致reduce阶段重复计算
					// 但是rename会覆盖同名文件，所以不需要担心这个问题，在reduce stage的时候只有一个唯一的intermediate文件
					os.Rename(intermediateFiles[i].Name(), fmt.Sprintf("mr-%d-%d", reply.taskId, i))
					intermediateFiles[i].Close()
				}
				ok := call("Worker.reportTaskCompletion", &MapDoneArgs{taskType: "map", TaskId: reply.taskId}, &RPCReply{})
				if !ok {
					log.Fatalf("call reportTaskCompletion failed")
				}
				
			case "reduce":
				// fmt.Printf("Worker %d received a reduce task with id %d and files %v\n", os.Getpid(), reply.taskId, reply.files)
				// reply.taskId is the unique identifier of intermediate files. Any intermediate file alike pattern "mr-*-reply.taskId" will be used for reduce task.
				pattern := fmt.Sprintf("mr-*-%d", reply.taskId)
				intermediateFiles, err := filepath.Glob(pattern)
				if err != nil {
					log.Fatalf("cannot find intermediate files for pattern %v", pattern)
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
				reduceHelper(reducef, reply.taskId, kva)
				ok := call("Worker.reportTaskCompletion", &MapDoneArgs{taskType: "reduce", TaskId: reply.taskId}, &RPCReply{})
				if !ok {
					log.Fatalf("call reportTaskCompletion failed")
				}
			case "wait":
				// fmt.Printf("Worker %d received a wait signal\n", os.Getpid())
				// Wait for a while before asking for a new task
				time.Sleep(1 * time.Second)
			case "exit":
				fmt.Printf("Worker %d received exit signal\n", os.Getpid())
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

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
