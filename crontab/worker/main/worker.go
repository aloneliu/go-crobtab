package main

import (
	"runtime"
	"flag"
	"log"
	"time"
	"go-crontab-work/crontab/worker"
	"fmt"
)

// 初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var configFile string
// 解析命令行参数
func initArgs() {
	flag.StringVar(&configFile, "config", "./worker.json", "worker文件的路径")
	flag.Parse()
}

func main() {
	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置
	err := worker.InitConfig(configFile)

	// 启动mongodb
	err = worker.InitLogSink()

	// 启动调度器
	err = worker.InitScheduler()

	// etcd 任务管理器
	err = worker.InitJobManager()

	// 执行器
	err = worker.InitExecutor()

	fmt.Println("work start...")

	if err != nil {
		log.Fatal(err)
	}

	for {
		time.Sleep(1 * time.Second)
	}

}
