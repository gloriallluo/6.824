package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// XXX: issue tasks in a pipelined fashion
const (
	MapPhase int = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	mu      sync.Mutex
	phase   int
	nw      WorkerId // #workers
	nm      int      // #maps
	nr      int      // #reduces
	maps    map[TaskId]WorkerId
	reduces map[TaskId]WorkerId
	files   []string
}

// -- RPC handlers for the worker to call.

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Id = c.nw
	reply.NMap = c.nm
	reply.NReduce = c.nr
	c.nw++
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// handle finished task
	c.handleFinishedTask(args.Prev)

	// get a new task
	switch c.phase {
	case MapPhase:
		reply.Result = c.getMapTask(args.Id, &reply.Task)
	case ReducePhase:
		reply.Result = c.getReduceTask(args.Id, &reply.Task)
	case DonePhase:
		reply.Result = None
	default:
		panic("no such phase")
	}
	return nil
}

func (c *Coordinator) handleFinishedTask(task Task) {
	log.Printf("Coordinator got one finished task %v", task)
	switch task.Name {
	case "map":
		delete(c.maps, task.Id)
		if len(c.maps) == 0 {
			c.phase = ReducePhase
			log.Printf("Coordinator turns to ReducePhase")
		}
	case "reduce":
		delete(c.reduces, task.Id)
		if len(c.reduces) == 0 {
			c.phase = DonePhase
			log.Printf("Coordinator turns to DonePhase")
		}
	case "none":
	default:
		panic("no such name")
	}
}

func (c *Coordinator) getMapTask(worker WorkerId, task *Task) GetTaskRes {
	for t := range c.maps {
		if c.maps[t] == NoWorker {
			task.Id = t
			task.Name = "map"
			task.FileName = c.files[int(t)]
			c.maps[t] = worker
			return Ok
		}
	}
	return Wait
}

func (c *Coordinator) getReduceTask(worker WorkerId, task *Task) GetTaskRes {
	for t := range c.reduces {
		if c.reduces[t] == NoWorker {
			task.Id = t
			task.Name = "reduce"
			c.reduces[t] = worker
			return Ok
		}
	}
	if len(c.reduces) == 0 {
		return None
	} else {
		return Wait
	}
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
	log.Printf("Coordinator listens to socket %v", sockname)
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == DonePhase
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.phase = MapPhase
	c.mu = sync.Mutex{}
	c.nw = 0
	c.nm = len(files)
	c.nr = nReduce
	c.maps = make(map[TaskId]WorkerId)
	c.reduces = make(map[TaskId]WorkerId)
	var task TaskId
	for task = 0; task < TaskId(c.nm); task++ {
		c.maps[task] = NoWorker
	}
	for task = 0; task < TaskId(c.nr); task++ {
		c.reduces[task] = NoWorker
	}
	log.Printf("Coordinator start, there are %d map tasks and %d reduce tasks", c.nm, c.nr)
	c.files = files

	c.server()
	return &c
}
