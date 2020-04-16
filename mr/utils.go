package mr

const (
	MAX_ERROR_COUNT = 2
)

const (
	TASK_ERROR      = -1
	TASK_NOT_READY  = 0
	TASK_IDLE       = 1
	TASK_INPROGRESS = 2
	TASK_COMPLETED  = 3
)

const (
	MAP_PHASE    = 0
	REDUCE_PHASE = 1
)

const (
	NODE_IDLE       = 0
	NODE_INPROGRESS = 1
	NODE_FAILURE    = 2
	NODE_COMPLETED  = 3
)

type Task struct {
	Phase    int
	DataPath string
	Id       int
	Status   int
	CheckSum int
}

type TaskStatus struct {
	Status     int
	CheckSum   int
	ErrorCount int
}

type Node struct {
	status   int
	nextFree int
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
