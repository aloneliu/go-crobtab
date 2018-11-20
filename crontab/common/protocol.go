package common

import (
	"encoding/json"
	"strings"
	"github.com/gorhill/cronexpr"
	"time"
	"context"
)

// 定时任务
type Job struct {
	Name     string `json:"name"`     // 任务名
	Command  string `json:"command"`  // shell命令
	CronExpr string `json:"cronExpr"` // cron表达式
}

// 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo
	OutPut      []byte // 脚本输出
	Err         error
	StartTime   time.Time
	EndTime     time.Time
}

// 任务调度计划
type JobSchedulerPlan struct {
	Job      *Job                 // 任务信息
	Expr     *cronexpr.Expression // 解析cron表达式
	NextTime time.Time            // 下次调度时间
}

// 任务执行状态
type JobExecuteInfo struct {
	Job        *Job
	PlanTime   time.Time          // 理论上的调度时间
	RealTime   time.Time          // 实际上的调度时间
	Ctx        context.Context    // 任务command的上下文
	CancelFunc context.CancelFunc // 用于任务command的cancel函数
}

// 变化事件
type JobEvent struct {
	EventType int // save delete
	Job       *Job
}

// http应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

// mongodb
type JobLog struct {
	JobName      string `bson:"jobName"`
	Command      string `bson:"command"`
	Err          string `bson:"err"`
	OutPut       string `bson:"outPut"`
	PlanTime     int64  `bson:"planTime"`
	ScheduleTime int64  `bson:"scheduleTime"`
	StartTime    int64  `bson:"startTime"`
	EndTime      int64  `bson:"endTime"`
}

type LogBatch struct {
	Logs []interface{}
}

func BuildResponse(errno int, msg string, data interface{}) ([]byte, error) {
	var response Response
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	return json.Marshal(response)
}

// 反序列化
func UnpackJob(value []byte) (*Job, error) {
	var job *Job
	err := json.Unmarshal(value, &job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// 从etcd中提取任务名 /cron/jobs/job1 抹掉/cron/jobs/
func ExtraJobName(s string) (string) {
	return strings.TrimPrefix(s, JOB_SAVE_DIR)
}

func ExtraKillerJobName(s string) (string) {
	return strings.TrimPrefix(s, JOB_KILLER_DIR)
}

// 任务事件变化 1)更新任务 2)删除任务
func BuildJobEvent(eventType int, job *Job) (*JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

// 构造任务执行信息
func BuildJobExecuteInfo(plan *JobSchedulerPlan) (*JobExecuteInfo) {
	jobExecuteInfo := &JobExecuteInfo{
		Job:      plan.Job,
		PlanTime: plan.NextTime,
		RealTime: time.Now(),
	}
	jobExecuteInfo.Ctx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())

	return jobExecuteInfo
}
