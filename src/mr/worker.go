package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type workerImpl struct {
	id WorkerId
	mu sync.Mutex
	mf func(string, string) []KeyValue // map func
	rf func(string, []string) string   // reduce func
	nm int                             // #maps
	nr int                             // #reduces
}

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

	worker := workerImpl{}
	worker.mf = mapf
	worker.rf = reducef

	// get a WorkerId
	worker.register()
	worker.work()
}

func (w *workerImpl) register() { w.callRegister() }

func (w *workerImpl) work() {
	prev := Task{Name: "none"}
	var next Task
	for {
		switch w.callGetTask(prev, &next) {
		case Ok:
			switch next.Name {
			case "map":
				w.doMap(next.Id, next.FileName)
			case "reduce":
				w.doReduce(next.Id)
			default:
				panic("no such task")
			}
			prev = next
		case None:
			prev = Task{Name: "none"}
			break
		case Wait:
			prev = Task{Name: "none"}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (w *workerImpl) doMap(id TaskId, file string) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Error occurred opening input file: %v", err)
	}

	kvs := w.mf(file, string(content))

	// intermediate files
	files := make([]*os.File, w.nr)
	encs := make([]*json.Encoder, w.nr)

	for i := 0; i < w.nr; i++ {
		n := fmt.Sprintf("mr-%d-%d", id, i)
		files[i], err = os.Create(n)
		if err != nil {
			log.Fatalf("Error occurred creating intermediate files: %v", err)
		}
		encs[i] = json.NewEncoder(files[i])
	}
	// close all files before return
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()

	// dump intermediate result
	for _, kv := range kvs {
		r := ihash(kv.Key) % w.nr
		encs[r].Encode(&kv)
	}
}

func (w *workerImpl) doReduce(id TaskId) {
	oname := fmt.Sprintf("mr-out-%d", id)
	fout, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Error occurred creating output file: %v", err)
	}
	defer fout.Close()

	// Key -> list(Value)
	kvs := make(map[string][]string)

	for i := 0; i < w.nm; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, id)
		fin, err := os.Open(iname)
		if err != nil {
			log.Fatalf("Error occurred opening itermediate result: %v", err)
		}
		defer fin.Close()
		dec := json.NewDecoder(fin)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			vs, ok := kvs[kv.Key]
			if ok {
				kvs[kv.Key] = append(vs, kv.Value)
			} else {
				kvs[kv.Key] = make([]string, 1)
				kvs[kv.Key][0] = kv.Value
			}
		}
	}

	// dump output files
	for k := range kvs {
		v := w.rf(k, kvs[k])
		fout.WriteString(fmt.Sprintf("%s %s\n", k, v))
	}
}

// --- RPCs ---

func (w *workerImpl) callRegister() {
	a := RegisterArgs{}
	r := RegisterReply{}
	if ok := call("Coordinator.Register", &a, &r); ok {
		w.id = r.Id
		w.nm = r.NMap
		w.nr = r.NReduce
	} else {
		log.Panicf("Something wrong happened during register")
	}
}

func (w *workerImpl) callGetTask(prev Task, next *Task) GetTaskRes {
	a := GetTaskArgs{Id: w.id, Prev: prev}
	r := GetTaskReply{}
	if ok := call("Coordinator.GetTask", &a, &r); ok {
		if r.Result == Ok {
			*next = r.Task
		}
		return r.Result
	}
	// the RPC call failed, assume the coordinator exited
	return None
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
		log.Fatalf("dialing error: %v", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}
