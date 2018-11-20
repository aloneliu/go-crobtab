package master

import (
	"net/http"
	"net"
	"time"
	"log"
	"strconv"
	"encoding/json"
	"go-crontab-work/crontab/common"
)

var (
	// 单例对象
	G_ApiServer *ApiServer
)

type ApiServer struct {
	httpServer *http.Server
}

// 初始化api服务
func InitApiServer() {
	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/job/save", handleJobServer)
	serveMux.HandleFunc("/job/delete", handleJobDelete)
	serveMux.HandleFunc("/job/list", handleJobList)
	serveMux.HandleFunc("/job/kill", handleJobKiller)

	// 静态文件目录
	dir := http.Dir(G_config.WebRoot)
	fileServer := http.FileServer(dir)
	serveMux.Handle("/", http.StripPrefix("/", fileServer))

	// 启动tcp监听
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(G_config.Port))
	if err != nil {
		log.Fatal("无法开启服务: ", err)
	}

	// 创建http服务
	httpServer := &http.Server{
		ReadTimeout:  time.Duration(G_config.ReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.WriteTimeout) * time.Millisecond,
		Handler:      serveMux,
	}

	// 单例赋值
	G_ApiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 服务端启动
	go httpServer.Serve(listener)
}

// 保存任务接口
// post job={name:"job", command:"command", cronExpr:"cron表达式"}
func handleJobServer(w http.ResponseWriter, r *http.Request) {
	// Content-Type: text/html; charset=UTF-8
	w.Header().Set("Content-Type", "application/json")
	// 解析post表单
	r.ParseForm()
	postJob := r.PostForm.Get("job")

	var job common.Job
	json.Unmarshal([]byte(postJob), &job)

	// 保存到etcd
	oldJob, err := G_JobManager.JobSave(&job)
	if err == nil {
		resp, _ := common.BuildResponse(0, "success", oldJob)

		w.Write(resp)
	} else {
		resp, _ := common.BuildResponse(-1, err.Error(), oldJob)

		w.Write(resp)
	}
}

// 删除任务接口
// post name = job name
func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// 解析post表单
	r.ParseForm()
	postName := r.PostForm.Get("name")

	// 保存到etcd
	oldJob, err := G_JobManager.JobDelete(postName)
	if err == nil {
		resp, _ := common.BuildResponse(0, "success", oldJob)

		w.Write(resp)
	} else {
		resp, _ := common.BuildResponse(-1, err.Error(), oldJob)

		w.Write(resp)
	}
}

// 任务列表
func handleJobList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	jobList, err := G_JobManager.JobList()

	if err == nil {
		resp, _ := common.BuildResponse(0, "success", jobList)

		w.Write(resp)
	} else {
		resp, _ := common.BuildResponse(-1, err.Error(), jobList)

		w.Write(resp)
	}
}

// 杀死任务
func handleJobKiller(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	r.ParseForm()
	postName := r.PostForm.Get("name")
	err := G_JobManager.KillerJob(postName)

	if err == nil {
		resp, _ := common.BuildResponse(0, "success", nil)

		w.Write(resp)
	} else {
		resp, _ := common.BuildResponse(-1, err.Error(), nil)

		w.Write(resp)
	}
}
