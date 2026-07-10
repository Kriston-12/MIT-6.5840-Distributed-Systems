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
	File      string
	State     TaskState
	StartTime time.Time
	Attempt   int
}

type Coordinator struct {
	mu          sync.Mutex
	phase       Phase
	nMap        int
	nReduce     int
	mapTasks    []Task
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
	if c.phase == Map {
		for i, task := range c.mapTasks {
			if task.State == Idle {
				c.mapTasks[i].State = InProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.TaskType = "map"
				reply.TaskId = i
				reply.MapFile = task.File
				reply.NReduce = c.nReduce
				reply.Attempt = c.mapTasks[i].Attempt
				log.Printf("ASSIGN type=%s task=%d start=%s attempt=%d", reply.TaskType, reply.TaskId, c.mapTasks[i].StartTime.Format(time.RFC3339Nano), reply.Attempt)
				return nil
			}
		}
		// If no idle map tasks, let workers wait
		reply.TaskType = "wait"
		return nil
	} else if c.phase == Reduce {
		for i, task := range c.reduceTasks {
			if task.State == Idle {
				c.reduceTasks[i].State = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.TaskType = "reduce"
				reply.TaskId = i
				reply.NReduce = c.nReduce
				reply.Attempt = c.reduceTasks[i].Attempt
				log.Printf("ASSIGN type=%s task=%d start=%s attempt=%d", reply.TaskType, reply.TaskId, c.reduceTasks[i].StartTime.Format(time.RFC3339Nano), reply.Attempt)
				return nil
			}
		}
		// If no idle reduce tasks, let workers wait
		reply.TaskType = "wait"
		return nil
	}
	// If the phase is AllDone, tell workers to exit
	reply.TaskType = "exit"
	return nil
}

func (c *Coordinator) ReportTaskCompletion(args *DoneArgs, reply *RPCReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == "map" {
		task := &c.mapTasks[args.TaskId]
		if task.Attempt != args.Attempt {
			log.Printf("REJECT type=%s task=%d attempt=%d expected=%d", args.TaskType, args.TaskId, args.Attempt, task.Attempt)
			return nil
		}
		task.State = Completed
		for _, task := range c.mapTasks {
			if task.State != Completed {
				return nil
			}
		}
		c.phase = Reduce
	} else if args.TaskType == "reduce" {
		task := &c.reduceTasks[args.TaskId]
		if task.Attempt != args.Attempt {
			log.Printf("REJECT type=%s task=%d attempt=%d expected=%d", args.TaskType, args.TaskId, args.Attempt, task.Attempt)
			return nil
		}
		task.State = Completed
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
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	if c.phase == AllDone {
		ret = true
	}

	return ret
}

func (c *Coordinator) expireTasksLocked(tasks []Task, taskType string) {
	for i := range tasks {
		task := &tasks[i]
		if task.State == InProgress {
			elapsed := time.Since(task.StartTime)
			if elapsed > 10*time.Second {
				task.State = Idle
				log.Printf("TIMEOUT type=%s task=%d attempt=%d elapsed=%s", taskType, i, task.Attempt, elapsed)
				
				task.Attempt++
				task.State = Idle
			}
		}
	}
}

// check if any in-progress tasks have timed out and reset them to idle
func (c *Coordinator) CheckTimeout() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		if c.phase == Map {
			c.expireTasksLocked(c.mapTasks, "map")
		} else if c.phase == Reduce {
			c.expireTasksLocked(c.reduceTasks, "reduce")
		} else if c.phase == AllDone {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:       Map,
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	// Your code here.
	// 下面的写法是错的，task.State是在修改副本
	// _, task := range ...这里task是原切片元素的value copy
	// for _, task := range c.mapTasks {
	// 	task.State = Idle

	// }

	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i].File = files[i]
		c.mapTasks[i].State = Idle
	}

	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i].State = Idle
	}

	c.server(sockname)

	go c.CheckTimeout()

	return &c
}
