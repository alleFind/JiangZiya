package leader_log

import (
	"JiangZiya/params"
	"io"
	"log"
	"os"
)

type LeaderLog struct {
	Llog *log.Logger
}

func NewLeaderLog() *LeaderLog {
	writer1 := os.Stdout

	dirpath := params.LogWrite_path

	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	writer2, err := os.OpenFile(dirpath+"/Leader.log", os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Panic(err)
	}

	// 将日志同时打印在控制台和日志文件里
	print := log.New(io.MultiWriter(writer1, writer2), "Leader: ", log.Lshortfile|log.Ldate|log.Ltime)
	return &LeaderLog{
		Llog: print,
	}
}
