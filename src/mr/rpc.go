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
	taskType string // "map", "reduce", "wait", "exit"
	mapFile string // one input file for map task 
	nReduce int // number of reduce tasks
	taskId int // task id for map or reduce task. 
			// for map task, it is the index of input file. map task produces itermediate files with name "mr-taskId-reduceTaskId(don't care in map phase)"
			// for reduce task, it is the reduceTaskId in map phase. worker will use "mr-(don't care)-reduceTaskId" to reduce all intermediate files with such patter.
}

type DoneArgs struct {
	taskType string
	TaskId int
}


