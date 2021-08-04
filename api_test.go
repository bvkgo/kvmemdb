package kvmemdb

import "github.com/bvkgo/kv"

var (
	_ kv.Reader      = &Tx{}
	_ kv.Writer      = &Tx{}
	_ kv.Deleter     = &Tx{}
	_ kv.Scanner     = &Tx{}
	_ kv.Iterator    = &Tx{}
	_ kv.Transaction = &Tx{}
)
