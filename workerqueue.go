package Orangedb

import (
	"runtime"
	"sync"
)


/*@berif  多个生产者 对 ConcurrencyLimit 个消费者*/
type WorkerQueue struct {
	Handler          func(interface{})  // task处理函数
	ConcurrencyLimit int   //最大的并发数
	push             chan interface{}
	pop              chan struct{}
	suspend          chan bool
	suspended        bool
	stop             chan struct{}
	stopped          bool
	buffer           []interface{}  // 当前所有task数组  //todo（shili） 将 buffer 改成环形队列，提高效率
	count            int     //当前正在执行的task数量
	wg               sync.WaitGroup
}

// Queue is the queue
// Queue also has the members Handler and ConcurrencyLimit which can be set at anytime
type QueueThread struct {
	*WorkerQueue
}

// Handler is a function that takes any value, and is called every time a task is processed from the queue
type Handler func(interface{})

// NewQueue must be called to initialize a new queue.
// The first argument is a Handler
// The second argument is an int which specifies how many operation can run in parallel in the queue, zero means unlimited.
func NewQueueThread(handler Handler, concurrencyLimit int ) *QueueThread {

	q := &QueueThread{
		&WorkerQueue{
			Handler:          handler,
			ConcurrencyLimit: concurrencyLimit,
			push:             make(chan interface{}),
			pop:              make(chan struct{}),
			suspend:          make(chan bool),
			stop:             make(chan struct{}),
		},
	}

	go q.run()  //start to handle task
	runtime.SetFinalizer(q, (*QueueThread).Stop)
	return q
}

// Push pushes a new value to the end of the queue
//note: 该队列只能添加，不能用户删除，只能处理后自己删除
func (q *QueueThread) Push(val interface{}) {
	q.push <- val
}

// Stop stops the queue from executing anymore tasks, and waits for the currently executing tasks to finish.
// The queue can not be started again once this is called
func (q *QueueThread) Stop() {

	q.stop <- struct{}{}
	runtime.SetFinalizer(q, nil)
}

// Wait calls wait on a waitgroup, that waits for all items of the queue to finish execution
func (q *QueueThread) Wait() {

	q.wg.Wait()
}

// Count returns the number of currently executing tasks and the number of tasks waiting to be executed
func (q *QueueThread) Len() (_, _ int) {

	return q.count, len(q.buffer)
}


/*@berif 开启线程处理*/
func (q *WorkerQueue) run() {

	defer func() {
		q.wg.Add(-len(q.buffer))
		q.buffer = nil
	}()
	for {

		select {

		case val := <-q.push:
			q.buffer = append(q.buffer, val)
			q.wg.Add(1)
		case <-q.pop:
			q.count--
		case suspend := <-q.suspend:
			if suspend != q.suspended {

				if suspend {
					q.wg.Add(1)
				} else {
					q.wg.Done()
				}
				q.suspended = suspend
			}
		case <-q.stop:
			q.stopped = true
		}

		if q.stopped && q.count == 0 {
			return
		}

		for (q.count < q.ConcurrencyLimit || q.ConcurrencyLimit == 0) && len(q.buffer) > 0 && !(q.suspended || q.stopped) {

			val := q.buffer[0]
			q.buffer = q.buffer[1:]
			q.count++
			go func() {
				defer func() {
					q.pop <- struct{}{}
					q.wg.Done()
				}()
				q.Handler(val)

			}()
		}

	}

}