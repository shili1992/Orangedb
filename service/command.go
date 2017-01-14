package service

import (
	"github.com/laohanlinux/go-logger/logger"
	"github.com/shili1992/Orangedb/common"
	"github.com/siddontang/go/hack"
)

type CommandFunc func(c *RespClient) error

var commandFuncMap = map[string]CommandFunc{}

func respGetCommand(c *RespClient) error {
	if len(c.args) < 1 {
		return common.ErrInvalidArgument
	}
	value, err := c.service.store.Get(string(c.args[0]))
	logger.Debugf("get  a key:value [%s:%s],err:%s", string(c.args[0]), value, err)
	if nil != err {
		logger.Infof("fail")
		return err
	} else {
		c.respWriter.writeBulk(hack.Slice(value))
		c.respWriter.flush()
	}
	return nil
}

func respSetCommand(c *RespClient) error {
	if len(c.args) < 2 {
		return common.ErrInvalidArgument
	}
	err := c.service.store.Set(string(c.args[0]), string(c.args[1]))
	logger.Debugf("set  a key:value [%s:%s] err:%s ", string(c.args[0]), string(c.args[1]), err)
	if nil != err {

		return err
	} else {
		c.respWriter.writeStatus(common.ResponseOK)
		c.respWriter.flush()
	}
	return nil
}

func respDeleteCommand(c *RespClient) error {
	if len(c.args) < 1 {
		return common.ErrInvalidArgument
	}
	err := c.service.store.Delete(string(c.args[0]))
	logger.Debugf("delete  a key[%s]", string(c.args[0]))
	if nil != err {
		return err
	} else {
		c.respWriter.writeStatus(common.ResponseOK)
		c.respWriter.flush()
	}
	return nil
}

/*@berif 注册各种回调函数*/
func Init() {
	commandFuncMap["get"] = respGetCommand
	commandFuncMap["delete"] = respDeleteCommand
	commandFuncMap["set"] = respSetCommand
}
