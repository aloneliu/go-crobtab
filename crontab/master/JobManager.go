package master

import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"go-crontab-work/crontab/common"
	"encoding/json"
	"context"
)

type JobManager struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
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

	G_JobManager = &JobManager{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return nil
}

// 保存job到etcd
func (this *JobManager) JobSave(job *common.Job) (oldJob *common.Job, err error) {
	// 保存到/cron/jobs/任务名
	jobKey := common.JOB_SAVE_DIR + job.Name

	jobValue, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}

	response, err := this.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}

	// 如果是更新 那么返回旧值
	if response.PrevKv != nil {
		e := json.Unmarshal(response.PrevKv.Value, &oldJob)
		if e != nil {
			return nil, nil
		}

		return oldJob, nil
	}

	return
}

// etcd删除任务
func (this *JobManager) JobDelete(name string) (oldJob *common.Job, err error) {
	jobKey := common.JOB_SAVE_DIR + name
	response, err := this.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}

	if len(response.PrevKvs) != 0 {
		e := json.Unmarshal(response.PrevKvs[0].Value, &oldJob)
		if e != nil {
			return nil, e
		}
		return oldJob, nil
	}

	return
}

// 获取任务列表
func (this *JobManager) JobList() ([]*common.Job, error) {
	dirKey := common.JOB_SAVE_DIR
	response, err := this.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	jobList := make([]*common.Job, 0)

	for _, v := range response.Kvs {
		job := &common.Job{}
		err := json.Unmarshal(v.Value, job)
		if err != nil {
			continue
		}
		jobList = append(jobList, job)
	}

	return jobList, nil
}

// 杀死任务
func (this *JobManager) KillerJob(name string) (error) {
	killerKey := common.JOB_KILLER_DIR + name

	// 让worker监听到一次put操作, 创建一个租约让其稍后自动过期即可 (1s)
	leaseGrantResponse, err := this.lease.Grant(context.TODO(), 1)
	if err != nil {
		return err
	}

	leaseID := leaseGrantResponse.ID

	_, err = this.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseID))
	if err != nil {
		return err
	}

	return nil
}
