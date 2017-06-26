package Orangedb

import (
	"sync"
)

/*环形队列*/
// Queue represents a single instance of the queue data structure.
type Queue struct {
	buf     []interface{}
	head	int     //指向 队头
	tail    int    //指向最后一个元素 +1
	count   int   //当前元素的数量
	mutex   sync.RWMutex
}

/*@berif  New constructs and returns a new Queue.
*/
func NewQueue() *Queue {
	return &Queue{
		buf: make([]interface{}, minQueueLen),
	}
}

// Length returns the number of elements currently stored in the queue.
func (q *Queue) Len() int {
	q.mutex.RLock()
	defer  q.mutex.RUnlock()
	return q.count
}

//Empty returns  if the queue has not any elements
func (q *Queue) Empty() bool{
	q.mutex.RLock()
	defer  q.mutex.RUnlock()
	return q.count==0
}

// resizes the queue to fit exactly twice its current contents
// this can result in shrinking if the queue is less than half-full
func (q *Queue) resize() {
	newBuf := make([]interface{}, q.count<<1)

	if q.tail > q.head {
		copy(newBuf, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuf, q.buf[q.head:])
		copy(newBuf[n:], q.buf[:q.tail])
	}

	q.head = 0
	q.tail = q.count
	q.buf = newBuf
}

// Add puts an element on the end of the queue.
func (q *Queue) Put(elem interface{}) {
	q.mutex.Lock()
	defer  q.mutex.Unlock()

	if q.count == len(q.buf) {
		q.resize()
	}

	q.buf[q.tail] = elem
	// bitwise modulus
	q.tail = (q.tail + 1) & (len(q.buf) - 1)
	q.count++
}

// Peek returns the element at the head of the queue. This call panics
// if the queue is empty.
func (q *Queue) Front() interface{} {
	q.mutex.RLock()
	defer  q.mutex.RUnlock()
	if q.count <= 0 {
		panic("queue: Peek() called on empty queue")
	}
	return q.buf[q.head]
}


// Peek returns the element at the tail of the queue. This call panics
// if the queue is empty.
func (q *Queue) Back() interface{} {
	q.mutex.RLock()
	defer  q.mutex.RUnlock()
	if q.count <= 0 {
		panic("queue: back() called on empty queue")
	}
	return q.Get(q.count-1)
}



// Get returns the element at index i in the queue. If the index is
// invalid, the call will panic. This method accepts both positive and
// negative index values. Index 0 refers to the first element, and
// index -1 refers to the last.
func (q *Queue) Get(i int) interface{} {
	q.mutex.RLock()
	defer  q.mutex.RUnlock()

	// If indexing backwards, convert to positive index.
	if i < 0 {
		i += q.count
	}
	if i < 0 || i >= q.count {
		panic("queue: Get() called with index out of range")
	}
	// bitwise modulus
	return q.buf[(q.head+i)&(len(q.buf)-1)]
}


//delete one element  from queue
func (q *Queue) Delete(target int){
	if(0 == target){
		q.Pop()
		return
	}

	q.mutex.Lock()
	defer  q.mutex.Unlock()
	// If indexing backwards, convert to positive index.
	if target < 0 {
		target += q.count
	}
	if target < 0 || target >= q.count {
		panic("queue: Get() called with index out of range")
	}
	//move   element after target
	for i:= target;i< q.count-1;i++{
		q.buf[(q.head+ i)&(len(q.buf)-1)] = q.buf[(q.head+ i+1)&(len(q.buf)-1)]
	}
	q.tail= (q.head+q.count-1)&(len(q.buf)-1)
	q.buf[q.tail] = nil

	q.count--
	// Resize down if buffer 1/4 full.
	if len(q.buf) > minQueueLen && (q.count<<2) == len(q.buf) {
		q.resize()
	}
}

// Remove removes and returns the element from the front of the queue. If the
// queue is empty, the call will panic.
func (q *Queue) Pop() interface{} {
	q.mutex.Lock()
	defer  q.mutex.Unlock()
	if q.count <= 0 {
		panic("queue: Remove() called on empty queue")
	}
	ret := q.buf[q.head]
	q.buf[q.head] = nil
	// bitwise modulus
	q.head = (q.head + 1) & (len(q.buf) - 1)
	q.count--
	// Resize down if buffer 1/4 full.
	if len(q.buf) > minQueueLen && (q.count<<2) == len(q.buf) {
		q.resize()
	}
	return ret
}
