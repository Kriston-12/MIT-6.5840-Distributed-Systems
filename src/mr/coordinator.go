package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Phase int

const (
	Map Phase = iota
	Reduce
	AllDone
)

type Task struct {
	ID			int
	File 		string
	State		TaskState
	StartTime	time.Time
}

type Coordinator struct {
	mu 			sync.Mutex
	phase 		Phase
	nMap		int
	nReduce		int
	mapTasks	[]Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Assign(args *RPCArgs, reply *RPCReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if (c.phase == Map) {	
		for i, task := range c.mapTasks {
			if task.State == Idle {
				c.mapTasks[i].State = InProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.taskType = "map"
				reply.taskId = task.ID
				reply.mapFile = task.File
				reply.nReduce = c.nReduce
				return nil
			}
		}
		// If no idle map tasks, let workers wait
		reply.taskType = "wait"
		return nil
	} else if (c.phase == Reduce) {
		for i, task := range c.reduceTasks {
			if task.State == Idle {
				c.reduceTasks[i].State = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.taskType = "reduce"
				reply.taskId = task.ID
				reply.nReduce = c.nReduce
				return nil
			}
		}
		// If no idle reduce tasks, let workers wait
		reply.taskType = "wait"
		return nil
	}
	// If the phase is AllDone, tell workers to exit
	reply.taskType = "exit"
	return nil
}

func (c *Coordinator) reportTaskCompletion(args *DoneArgs, reply *RPCReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if (args.taskType == "map") {
		if time.Since(c.mapTasks[args.TaskId].StartTime) > 10*time.Second {
			log.Printf("Warning: Map task %d reported completion after timeout", args.TaskId)
			return nil // Ignore late completion reports
		}
		c.mapTasks[args.TaskId].State = Completed
		for _, task := range c.mapTasks {
			if task.State != Completed {
				return nil
			}
		}
		c.phase = Reduce
	} else if (args.taskType == "reduce") {
		if time.Since(c.reduceTasks[args.TaskId].StartTime) > 10*time.Second {
			log.Printf("Warning: Reduce task %d reported completion after timeout", args.TaskId)
			return nil // Ignore late completion reports
		}
		c.reduceTasks[args.TaskId].State = Completed
		for _, task := range c.reduceTasks {
			if task.State != Completed {
				return nil
			}
		}
		c.phase = AllDone
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.phase == AllDone {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server(sockname)
	return &c
}
