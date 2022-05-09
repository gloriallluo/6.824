package mr

type WorkerId int

const NoWorker WorkerId = -1

type TaskId int

type Task struct {
	Id       TaskId
	Name     string
	FileName string
}
