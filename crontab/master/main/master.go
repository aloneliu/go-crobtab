package main

import (
	"runtime"
	"go-crontab-work/crontab/master"
	"flag"
	"log"
	"time"
)

// 初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var configFile string
// 解析命令行参数
func initArgs() {
	flag.StringVar(&configFile, "config", "./master.json", "master文件的路径")
	flag.Parse()
}

func main() {
	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置
	err := master.InitConfig(configFile)
	if err != nil {
		log.Fatal(err)
	}

	// 任务管理器
	err = master.InitJobManager()
	if err != nil {
		log.Fatal(err)
	}

	// 启动api服务
	master.InitApiServer()

	for {
		time.Sleep(1 * time.Second)
	}

}
