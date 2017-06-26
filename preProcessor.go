package Orangedb

import (
	"github.com/laohanlinux/go-logger/logger"
)

type  PreProcessor struct {
	workqueue  *QueueThread
	sequencerApplyCh       chan *Txn
}

/*@berif start queue and 1 thread to handle tasks*/
func NewPreProcessor() *PreProcessor {
	logger.Infof("start preprocessor")
	p:=new(PreProcessor)
	p.workqueue=NewQueueThread(p.TaskHandler,1)
	return  p
}

func  (this *PreProcessor) setSequencerCh(applyCh chan *Txn){
	this.sequencerApplyCh = applyCh
}


/*preProcessor 处理函数*/
func  (this *PreProcessor) TaskHandler(task interface{})  {
	txn:=task.(*Txn)
	logger.Infof("preprocessor deliver one txn:%s",ToString(txn))
	this.sequencerApplyCh<-txn    //向 Sequencer 通道法一个txn
}


func (this *PreProcessor) Push(val interface{}) {
	this.workqueue.Push(val)
}

