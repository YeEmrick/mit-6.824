package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

type worker struct {
	nMap    int
	nReduce int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) applyTask() {
	args := Args{}
	reply := Reply{}
	if success := call("Master.ApplyTask", &args, &reply); success == false {
		fmt.Println("applyTask: call Master.ApplyTask error!")
		os.Exit(1)
	}
	w.nMap = reply.NMap
	w.nReduce = reply.NReduce
}

func (w *worker) getTask() *Task {
	args := Args{}
	reply := Reply{}
	if success := call("Master.GetTask", &args, &reply); success == false {
		fmt.Println("getTask: call Master.GetTask error!")
		os.Exit(1)
	}
	return reply.Task
}

func (w *worker) doTask(task *Task) {
	if task.Phase == MAP_PHASE {
		w.doMapTask(task)
	} else {
		w.doReduceTask(task)
	}
}

func (w *worker) putTask(task *Task) {
	args := Args{
		TaskPhase:  task.Phase,
		TaskId:     task.Id,
		TaskStatus: task.Status,
	}
	reply := Reply{}
	if success := call("Master.PutTask", &args, &reply); success == false {
		fmt.Println("putTask: call Master.PutTask error!")
		os.Exit(1)
	}
}

func (w *worker) doMapTask(task *Task) {
	file, err := os.Open(task.DataPath)
	if err != nil {
		fmt.Println("cannot open %v", task.DataPath)
		task.Status = TASK_ERROR
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("cannot read %v", task.DataPath)
		task.Status = TASK_ERROR
		return
	}
	if err := file.Close(); err != nil {
		fmt.Println("cannot Close %v", task.DataPath)
		task.Status = TASK_ERROR
		return
	}
	kva := w.mapf(task.DataPath, string(content))

	// split into nReduce files
	tmpKV := make([][]KeyValue, w.nReduce)
	for _, kv := range kva {
		tmpKV[ihash(kv.Key)%w.nReduce] = append(tmpKV[ihash(kv.Key)%w.nReduce], kv)
	}
	for reduceId, kvArray := range tmpKV {
		oname := fmt.Sprintf("mr-%d-%d", task.Id, reduceId)
		f, err := os.Create(oname)
		if err != nil {
			fmt.Println("cannot create %v", oname)
			task.Status = TASK_ERROR
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range kvArray {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("enc.Encode error")
				task.Status = TASK_ERROR
				return
			}
		}
		if err := f.Close(); err != nil {
			fmt.Println("cannot Close %v", oname)
			task.Status = TASK_ERROR
			return
		}
	}

	task.Status = TASK_COMPLETED
}

func (w *worker) doReduceTask(task *Task) {
	kva := []KeyValue{}
	for mapId := 0; mapId < w.nMap; mapId++ {
		dataPath := fmt.Sprintf("mr-%d-%d", mapId, task.Id)
		file, err := os.Open(dataPath)
		if err != nil {
			fmt.Println("cannot open %v", task.DataPath)
			task.Status = TASK_ERROR
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Println("cannot create %v", oname)
		task.Status = TASK_ERROR
		return
	}
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	if err := ofile.Close(); err != nil {
		fmt.Println("cannot Close %v", oname)
		task.Status = TASK_ERROR
		return
	}
	task.Status = TASK_COMPLETED
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := worker{
		mapf:    mapf,
		reducef: reducef,
		nReduce: -1,
		nMap:    -1,
	}

	for {
		w.applyTask()
		task := w.getTask()
		w.doTask(task)
		w.putTask(task)

		time.Sleep(5 * time.Second)
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
