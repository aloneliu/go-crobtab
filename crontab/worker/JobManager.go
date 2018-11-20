package worker

import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"go-crontab-work/crontab/common"
	"context"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type JobManager struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// 单例
	G_JobManager *JobManager
)

func InitJobManager() error {
	config := clientv3.Config{
		Endpoints:   G_config.EtcdEndPoint, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdTimeout) * time.Millisecond,
	}

	client, e := clientv3.New(config)
	if e != nil {
		return e
	}

	kv := client.KV
	lease := client.Lease
	watcher := client.Watcher

	G_JobManager = &JobManager{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	// 启动监听job
	G_JobManager.JobWatch()

	// 启动监听killer
	G_JobManager.KillerWatch()

	return nil
}

// 保存job到etcd
func (this *JobManager) JobWatch() (err error) {
	var jobEvent *common.JobEvent

	// 获取/cron/jobs/的所有任务
	getResponse, err := this.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	// 单前有哪些任务
	for _, v := range getResponse.Kvs {
		unpackJob, err := common.UnpackJob(v.Value)
		if err == nil {
			// todo: 把这个job同步到scheduler
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, unpackJob)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	go func() {
		// 从get的后续版本开始监听
		watchStartRevision := getResponse.Header.Revision + 1

		WatchChan := this.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())

		// 处理监听事件
		for watchResp := range WatchChan {
			for _, watchEvent := range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					// 任务保存
					unpackJob, err := common.UnpackJob(watchEvent.Kv.Value)
					if err != nil {
						continue
					}

					// 构造一个更新事件
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, unpackJob)
				case mvccpb.DELETE:
					// 任务删除
					jobName := common.ExtraJobName(string(watchEvent.Kv.Key))
					unpackJob := &common.Job{
						Name: jobName,
					}

					// 构造一个删除事件
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, unpackJob)
				}

				// todo: 推送给scheduler
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()

	return
}

// 创建任务执行锁
func (this *JobManager) CreateJobLock(jobName string) (*JobLock) {
	// 返回一把锁
	jobLock := InitJobLock(jobName, this.kv, this.lease)

	return jobLock
}

func (this *JobManager) KillerWatch() (err error) {
	var jobEvent *common.JobEvent

	go func() {
		// 从get的后续版本开始监听
		WatchChan := this.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix())

		// 处理监听事件
		for watchResp := range WatchChan {
			for _, watchEvent := range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					// 杀死某个任务
					jobName := common.ExtraKillerJobName(string(watchEvent.Kv.Key))
					job := &common.Job{
						Name: jobName,
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
				case mvccpb.DELETE:
					// killer标记过期 被自动删除
				}

				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()

	return
}
