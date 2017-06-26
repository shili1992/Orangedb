package Orangedb


// minQueueLen is smallest capacity that queue may have.
// Must be power of 2 for bitwise modulus: x % n == x & (n - 1).
const minQueueLen = 1024

// Queue represents a single instance of the queue data structure.
type FixedQueue struct {
	buf     []interface{}
	head	int
	tail    int
	capacity   int   //队列当前长度
}

/*@berif  NewFixedQueue constructs and returns a new Queue.
* @param  size of new Queue（2^n is request）
*/
func NewFixedQueue(maxSize int) *FixedQueue {
	if(maxSize<minQueueLen){
		maxSize = minQueueLen;
	}
	return &FixedQueue{
		buf: make([]interface{}, maxSize),
		head:0,
		tail:0,
		capacity:maxSize,
	}
}

// Length returns the number of elements currently stored in the queue.
func (q *FixedQueue) GetTotal() int {
	return (q.tail+q.capacity -q.head)&(q.capacity- 1)
}

func (q *FixedQueue) GetFree() int {
	return q.capacity-q.GetTotal()
}


// Add puts an element on the end of the queue.
func (q *FixedQueue) Push(elem interface{})error {
	if q.GetTotal() == q.capacity{
		return ErrFullQueue
	}

	q.buf[q.tail] = elem
	// bitwise modulus
	q.tail = (q.tail + 1) & (q.capacity- 1) //取余
	return nil
}

// Peek returns the element at the head of the queue. This call panics
// if the queue is empty.
func (q *FixedQueue) Peek() (interface{},error) {
	if q.GetTotal()  <= 0 {
		return nil,ErrEmptyQueue
	}
	return q.buf[q.head],nil
}

// Get returns the element at index i in the queue. If the index is
// invalid, the call will panic. This method accepts both positive and
// negative index values. Index 0 refers to the first element, and
// index -1 refers to the last.
func (q *FixedQueue) Get(i int) (interface{},error) {
	// If indexing backwards, convert to positive index.
	if i < 0 {
		i += q.GetTotal()
	}
	if i < 0 || i >= q.GetTotal() {
		return nil,ErrOutOfRange
	}
	// bitwise modulus
	return q.buf[(q.head+i)&(q.capacity-1)],nil
}

// Remove removes and returns the element from the front of the queue. If the
// queue is empty, the call will panic.
func (q *FixedQueue) Remove() (interface{},error) {
	if q.GetTotal() <= 0 {
		return nil,ErrEmptyQueue
	}
	ret := q.buf[q.head]
	q.buf[q.head] = nil
	// bitwise modulus
	q.head = (q.head + 1) & (len(q.buf) - 1)

	return ret,nil
}
