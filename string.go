package Orangedb

type ToStringConverter interface {
	ToString() string
}


func ToString(obj ToStringConverter) string{
	return obj.ToString()
}