package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	dataPath         []string
	taskCh           chan *Task
	NReduce          int
	NMap             int
	mu               sync.RWMutex
	phase            int
	mapTaskStatus    []*TaskStatus
	reduceTaskStatus []*TaskStatus
	finish           bool
}

func (m *Master) scheduleTask() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.phase == MAP_PHASE {
		finish := true
		for index, status := range m.mapTaskStatus {
			if status.Status != TASK_COMPLETED {
				finish = false
			}
			if status.Status == TASK_NOT_READY {
				task := &Task{
					Phase:    MAP_PHASE,
					DataPath: m.dataPath[index],
					Id:       index,
					CheckSum: os.Getuid(),
				}
				m.mapTaskStatus[index] = &TaskStatus{
					Status:     TASK_IDLE,
					CheckSum:   task.CheckSum,
					ErrorCount: 0,
				}
				m.taskCh <- task
			}
		}
		if finish {
			m.phase = REDUCE_PHASE
		}
		return
	}
	if m.phase == REDUCE_PHASE {
		finish := true
		for index, status := range m.reduceTaskStatus {
			if status.Status != TASK_COMPLETED {
				finish = false
			}
			if status.Status == TASK_NOT_READY {
				task := &Task{
					Phase:    REDUCE_PHASE,
					DataPath: "",
					Id:       index,
					CheckSum: os.Getuid(),
				}
				m.reduceTaskStatus[index] = &TaskStatus{
					Status:     TASK_IDLE,
					CheckSum:   task.CheckSum,
					ErrorCount: 0,
				}
				m.taskCh <- task
			}
		}
		if finish {
			m.finish = true
		}
		return
	}
}

func (m *Master) schedulePeriod() {
	for {
		m.scheduleTask()
		time.Sleep(5 * time.Second)
	}
}

func (m *Master) initTaskStatus() {
	m.mapTaskStatus = make([]*TaskStatus, m.NMap)
	m.reduceTaskStatus = make([]*TaskStatus, m.NReduce)
	for i, _ := range m.mapTaskStatus {
		m.mapTaskStatus[i] = &TaskStatus{
			Status:     TASK_NOT_READY,
			CheckSum:   0,
			ErrorCount: 0,
		}
	}
	for i, _ := range m.reduceTaskStatus {
		m.reduceTaskStatus[i] = &TaskStatus{
			Status:     TASK_NOT_READY,
			CheckSum:   0,
			ErrorCount: 0,
		}
	}
}

func (m *Master) checkPeriod() {
	for {
		m.mu.Lock()
		if m.phase == MAP_PHASE {
			for id, status := range m.mapTaskStatus {
				if status.ErrorCount > MAX_ERROR_COUNT {
					m.mapTaskStatus[id] = &TaskStatus{
						Status:     TASK_NOT_READY,
						CheckSum:   0,
						ErrorCount: 0,
					}
					continue
				}
				if status.Status == TASK_ERROR || status.Status == TASK_INPROGRESS {
					status.ErrorCount++
				}
			}
		} else {
			for id, status := range m.reduceTaskStatus {
				if status.ErrorCount > MAX_ERROR_COUNT {
					m.reduceTaskStatus[id] = &TaskStatus{
						Status:     TASK_NOT_READY,
						CheckSum:   0,
						ErrorCount: 0,
					}
					continue
				}
				if status.Status == TASK_ERROR || status.Status == TASK_INPROGRESS {
					status.ErrorCount++
				}
			}
		}
		m.mu.Unlock()
		time.Sleep(5 * time.Second)
	}
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *Args, reply *Reply) error {
	task := <-m.taskCh
	reply.Task = task
	m.mu.Lock()
	defer m.mu.Unlock()
	if task.Phase == MAP_PHASE {
		m.mapTaskStatus[task.Id].Status = TASK_INPROGRESS
	} else {
		m.reduceTaskStatus[task.Id].Status = TASK_INPROGRESS
	}
	return nil
}

func (m *Master) ApplyTask(args *Args, reply *Reply) error {
	m.scheduleTask()
	reply.NMap = m.NMap
	reply.NReduce = m.NReduce
	return nil
}

func (m *Master) PutTask(args *Args, reply *Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.TaskPhase == m.phase {
		if args.TaskPhase == MAP_PHASE {
			if args.TaskCheckSum != m.mapTaskStatus[args.TaskId].CheckSum {
				// discard the task
				return nil
			}
			m.mapTaskStatus[args.TaskId].Status = args.TaskStatus
		} else {
			if args.TaskCheckSum != m.reduceTaskStatus[args.TaskId].CheckSum {
				return nil
			}
			m.reduceTaskStatus[args.TaskId].Status = args.TaskStatus
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.finish
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	//init Master struct
	m.dataPath = files
	m.NMap = len(files)
	m.NReduce = nReduce
	m.mu = sync.RWMutex{}
	m.phase = MAP_PHASE
	m.finish = false
	m.initTaskStatus()
	if m.NMap > m.NReduce {
		m.taskCh = make(chan *Task, m.NMap)
	} else {
		m.taskCh = make(chan *Task, m.NReduce)
	}

	go m.schedulePeriod()
	go m.checkPeriod()

	m.server()
	return &m
}
