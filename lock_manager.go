package Orangedb

import "github.com/laohanlinux/go-logger/logger"

type  LockMode int
const (
	_ LockMode= iota
	UNLOCKED
	READ
	WRITE
)



type LockRequest struct{
	Txn  * Txn
	Mode LockMode
}


type KeyLockInfo struct {
	KeyLockQueue *Queue
}

func (k *KeyLockInfo) Empty() bool{
	return k.KeyLockQueue.Empty()
}

func (k *KeyLockInfo) Back() *LockRequest{
	return k.KeyLockQueue.Back().(*LockRequest)
}

func (k *KeyLockInfo) Put(r *LockRequest){
	k.KeyLockQueue.Put(r)
}

func (k *KeyLockInfo) Get(i int) *LockRequest{
	return k.KeyLockQueue.Get(i).(*LockRequest)
}


type LockManager struct {
	ReadyCh       chan struct{}   //向scheduler docheck 线程发送消息
	keyLockMap    map[string] *KeyLockInfo
	waitTxns      map[*Txn] int
	ReadyTxnQueue *Queue
	LocalPartitionID	int
	Chooser        *HashChooser   //分区方法
}

func NewLockManager()*LockManager {

	lockManager:=&LockManager{
		keyLockMap : make(map[string]*KeyLockInfo),
		waitTxns:    make(map[*Txn] int),
	}
	return lockManager
}


func  (l *LockManager)isLocal(key string) bool{
	return l.Chooser.Choose(key) == l.LocalPartitionID
}


func (l *LockManager)getKeyLock(key string) *KeyLockInfo {
	result,exist :=l.keyLockMap[key]
	if !exist{  //new    new keyLockinfo
		result = new(KeyLockInfo)
		result.KeyLockQueue = NewQueue()
		l.keyLockMap[key] = result
	}
	return result
}


func (l *LockManager)Lock(txn *Txn) int{

	notAcquired :=0
	for writeKey,_ := range txn.WriteSet{
		if l.isLocal(writeKey){
			keyLockInfo := l.getKeyLock(writeKey)
			if keyLockInfo.Empty()||keyLockInfo.Back().Txn !=txn {
				keyLockInfo.Put(&LockRequest{Txn:txn, Mode:WRITE})
			}
			if keyLockInfo.KeyLockQueue.Len()>1{
				notAcquired++
			}
		}
	}

	for readKey,_ := range txn.ReadSet{
		if l.isLocal(readKey) {
			keyLockInfo := l.getKeyLock(readKey)
			if keyLockInfo.Empty()||keyLockInfo.Back().Txn !=txn {
				keyLockInfo.Put(&LockRequest{Txn:txn, Mode:READ})
			}
			len:=keyLockInfo.KeyLockQueue.Len()
			for i:=0;i<len;i++{
				if WRITE==keyLockInfo.Get(i).Mode {
					notAcquired++
					break
				}
			}
		}
	}

	if notAcquired>0{
		l.waitTxns[txn]= notAcquired
	}else{
		l.ReadyTxnQueue.Put(txn)
		l.ReadyCh<- struct {}{}   //notify to hand ready txn
	}
	return notAcquired
}

func (l *LockManager)UnLock(txn *Txn) error{
	var ret error
	for writeKey,_ := range txn.WriteSet{
		if(l.isLocal(writeKey)){
			ret = l.ReleaseKeyLock(writeKey,txn)
		}
	}

	for readKey,_ := range txn.ReadSet{
		if(l.isLocal(readKey)) {
			ret =l.ReleaseKeyLock(readKey,txn)
		}
	}
	return ret
}


//release key lock (get by txn), single thread operate it
func (l *LockManager)ReleaseKeyLock(key string,txn *Txn) error{
	if !l.isLocal(key){
		return ErrNotLocalKey
	}
	write_precede_targe := false
	var target int =0
	newOwners := make([]*Txn,8)
	keyLockInfo := l.getKeyLock(key)

	for target = 0; target < keyLockInfo.KeyLockQueue.Len(); target++ {
		request:= keyLockInfo.Get(target)
		if request.Txn== txn{
			break
		}
		if WRITE==request.Mode{
			write_precede_targe = true
		}
	}

	if target>=keyLockInfo.KeyLockQueue.Len(){
		//don't find txn，do nothing
	}else if target+1 >= keyLockInfo.KeyLockQueue.Len(){//last one in queue
		//do nothing
	}else {
		next := target+1
		targetRequest:= keyLockInfo.Get(target)
		nextRequest:= keyLockInfo.Get(next)
		if target ==0{
			if WRITE ==targetRequest.Mode ||
			   (READ==targetRequest.Mode && WRITE==nextRequest.Mode){
				if WRITE==nextRequest.Mode{
					newOwners=append(newOwners,nextRequest.Txn)
				}
				for i:= next; i< keyLockInfo.KeyLockQueue.Len()&&READ ==keyLockInfo.Get(i).Mode; i++{
					newOwners=append(newOwners,keyLockInfo.Get(i).Txn)
				}
			}
		}else if !write_precede_targe && WRITE ==targetRequest.Mode&&READ ==nextRequest.Mode {
			for i:= next; i< keyLockInfo.KeyLockQueue.Len()&&READ ==keyLockInfo.Get(i).Mode; i++{
				newOwners=append(newOwners,keyLockInfo.Get(i).Txn)
			}
		}
	}

	for _,txn:= range newOwners{
		l.waitTxns[txn]--
		if 0 == l.waitTxns[txn]{
			l.ReadyTxnQueue.Put(txn)
			l.ReadyCh<- struct {}{}
			delete(l.waitTxns,txn)
		}
	}

	keyLockInfo.KeyLockQueue.Delete(target)
	logger.Infof("unlock,get one txn,%s  ,key:%s",ToString(txn),key)
	if(keyLockInfo.KeyLockQueue.Len()==0){
		delete(l.keyLockMap,key)
	}
	return  nil
}
