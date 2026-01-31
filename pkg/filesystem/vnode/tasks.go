package vnode

// TasksVNode provides a read-only /tasks directory.
// Task outputs will be populated by the task execution system.
type TasksVNode struct {
	ReadOnlyBase
}

func NewTasksVNode() *TasksVNode {
	return &TasksVNode{}
}

func (t *TasksVNode) Prefix() string { return TasksPath }

func (t *TasksVNode) Getattr(path string) (*FileInfo, error) {
	if path == TasksPath {
		return NewDirInfo(PathIno(path)), nil
	}
	return nil, ErrNotFound
}

func (t *TasksVNode) Readdir(path string) ([]DirEntry, error) {
	if path == TasksPath {
		return []DirEntry{}, nil
	}
	return nil, ErrNotFound
}

func (t *TasksVNode) Open(path string, flags int) (FileHandle, error) {
	return 0, ErrNotFound
}

func (t *TasksVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	return 0, ErrNotFound
}
