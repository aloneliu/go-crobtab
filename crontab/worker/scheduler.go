package worker

import (
	"go-crontab-work/crontab/common"
	"github.com/gorhill/cronexpr"
	"time"
	"fmt"
)

// 调度协程
type Scheduler struct {
	jobEventChan      chan *common.JobEvent // etcd任务事件队列
	jobPlanTable      map[string]*common.JobSchedulerPlan
	jobExecutingTable map[string]*common.JobExecuteInfo // 任务执行表
	jobExecuteResult  chan *common.JobExecuteResult     // 任务执行结果
}

var G_scheduler *Scheduler

// 初始化
func InitScheduler() error {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulerPlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobExecuteResult:  make(chan *common.JobExecuteResult, 1000),
	}

	go G_scheduler.schedulerLoop()

	return nil
}

// 调度协程
func (this *Scheduler) schedulerLoop() {
	// 初始化 1s
	scheduleAfter := this.TrySchedule()

	newTimer := time.NewTimer(scheduleAfter)

	for {
		select {
		case jobEvent := <-this.jobEventChan: // 监听任务变化事件
			// 维护的列表进行增删改查
			this.handleJobEvent(jobEvent)
		case <-newTimer.C: // 最近的任务到期了
		case results := <-this.jobExecuteResult: // 监听任务执行结果
			this.handleJobResult(results)
		}

		// 重新调度任务
		scheduleAfter = this.TrySchedule()

		// 重置调度间隔
		newTimer.Reset(scheduleAfter)
	}
}

// 推送任务变化事件
func (this *Scheduler) PushJobEvent(event *common.JobEvent) {
	this.jobEventChan <- event
}

// 处理任务事件
func (this *Scheduler) handleJobEvent(event *common.JobEvent) {
	switch event.EventType {
	case common.JOB_EVENT_SAVE: // 保存任务事件
		plan, err := BuildJobSchedulerPlan(event.Job)
		if err != nil {
			return
		}

		this.jobPlanTable[event.Job.Name] = plan
	case common.JOB_EVENT_DELETE: // 删除任务事件
		if _, ok := this.jobPlanTable[event.Job.Name]; ok {
			delete(this.jobPlanTable, event.Job.Name)
		}
	case common.JOB_EVENT_KILL: // 强杀任务事件
		// 取消command执行
		// 判断任务是否在执行
		if executeInfo, ok := this.jobExecutingTable[event.Job.Name]; ok {
			executeInfo.CancelFunc() // 触发command杀死shell子进程, 任务退出
		}
	}
}

// 构造任务执行计划 和etcd保存一致
func BuildJobSchedulerPlan(job *common.Job) (*common.JobSchedulerPlan, error) {
	expression, err := cronexpr.Parse(job.CronExpr)
	if err != nil {
		return nil, err
	}

	// 生成任务调度计划
	jobSchedulerPlan := &common.JobSchedulerPlan{
		Job:      job,
		Expr:     expression,
		NextTime: expression.Next(time.Now()),
	}

	return jobSchedulerPlan, nil
}

// 重新计算任务调度状态
func (this *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	// 遍历所有任务
	var nearTime *time.Time

	// 没有任务
	if len(this.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	now := time.Now()
	for _, v := range this.jobPlanTable {
		if v.NextTime.Before(now) || v.NextTime.Equal(now) {
			// 任务到期 尝试执行任务
			this.StartJob(v)
			v.NextTime = v.Expr.Next(now) // 更新下次执行时间
		}

		// 统计最近要过期的任务的时间 (N秒后过期)
		if nearTime == nil || v.NextTime.Before(*nearTime) {
			nearTime = &v.NextTime
		}
	}

	// 下次调度间隔 -单前时间
	scheduleAfter = (*nearTime).Sub(now)

	return
}

func (this *Scheduler) StartJob(plan *common.JobSchedulerPlan) {
	// 调度和执行是2件事
	// 一个任务可能运行很久, 但只能执行一次

	// 如果任务正在执行 跳过调度
	var (
		info *common.JobExecuteInfo
		ok   bool
	)
	if info, ok = this.jobExecutingTable[plan.Job.Name]; ok {
		fmt.Println("正在执行, 跳过#:", info.Job.Name)
		return
	}

	// 构建执行状态信息
	info = common.BuildJobExecuteInfo(plan)

	// 保存执行状态
	this.jobExecutingTable[plan.Job.Name] = info

	// 执行任务
	fmt.Println("执行任务: ", info.Job.Name, info.RealTime, info.PlanTime)
	G_Executor.ExecuteJob(info)
}

// 回传执行结果
func (this *Scheduler) PushJobResult(result *common.JobExecuteResult) {
	this.jobExecuteResult <- result
}

// 处理结果
func (this *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	delete(this.jobExecutingTable, result.ExecuteInfo.Job.Name)
	var jobLog *common.JobLog
	// 任务日志
	if result.Err != common.ERR_LOCK_ALRADY_REQUIRE {
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			OutPut:       string(result.OutPut),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      result.EndTime.UnixNano() / 1000 / 1000,
		}

		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}

		// 存到mongodb
		G_log_sink.Append(jobLog)
	}
}
