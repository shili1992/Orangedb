package Orangedb

import "fmt"

type Server  struct {
	Addr   		string
	clusterID	int
	partitionID	int
}


func (s Server) ToString() string{
	result:=fmt.Sprintf("Server[%s,Cluster:%d,Partition:%d]",s.Addr,s.clusterID,s.partitionID)
	return result
}