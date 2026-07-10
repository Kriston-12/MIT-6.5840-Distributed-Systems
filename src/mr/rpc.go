package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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
type RPCArgs struct {}

type RPCReply struct {
	TaskType string // "map", "reduce", "wait", "exit"
	MapFile string // one input file for map task 
	NReduce int // number of reduce tasks
	TaskId int // task id for map or reduce task. 
			// for map task, it is the index of input file. map task produces itermediate files with name "mr-taskId-reduceTaskId(don't care in map phase)"
			// for reduce task, it is the reduceTaskId in map phase. worker will use "mr-(don't care)-reduceTaskId" to reduce all intermediate files with such patter.
	Attempt int // attempt number for the task. If a worker fails to complete a task, the coordinator will assign the same task to another worker with attempt number increased by 1.
}

type DoneArgs struct {
	TaskType string
	TaskId int
	Attempt int
}


