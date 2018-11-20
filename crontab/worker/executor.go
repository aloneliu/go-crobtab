package worker

import (
	"go-crontab-work/crontab/common"
	"os/exec"
	"time"
)

type Executor struct {
}

var (
	G_Executor *Executor
)

// 初始化
func InitExecutor() (error) {
	G_Executor = &Executor{}

	return nil
}

// 执行一个任务
func (this *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	var result *common.JobExecuteResult

	go func() {
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			OutPut:      make([]byte, 0),
		}

		// 获取执行的锁
		jobLock := G_JobManager.CreateJobLock(info.Job.Name)

		err := jobLock.TryLock()
		// 释放锁
		defer jobLock.Unlock()

		if err != nil {
			// 上锁失败
			result.EndTime = time.Now()
			result.Err = err
		} else {
			// 任务开始时间
			result.StartTime = time.Now()

			commandContext := exec.CommandContext(info.Ctx, "/bin/bash", "-c", info.Job.Command)
			bytes, err := commandContext.Output()

			result.EndTime = time.Now()
			result.OutPut = bytes
			result.Err = err
		}
		G_scheduler.PushJobResult(result)
	}()
}
