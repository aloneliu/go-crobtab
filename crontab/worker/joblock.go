package worker

import (
	"go.etcd.io/etcd/clientv3"
	"context"
	"go-crontab-work/crontab/common"
)

type JobLock struct {
	kv         clientv3.KV
	lease      clientv3.Lease
	jobName    string             // 任务名
	cancelFunc context.CancelFunc // 用于终止自动续租
	leaseID    clientv3.LeaseID
	isLocked   bool // 是否上锁
}

// 初始化
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) *JobLock {
	jobLock := &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}

	return jobLock
}

// 尝试上锁
func (this *JobLock) TryLock() error {
	// 创建租约(5s)
	// 自动续租
	// 创建事务txn
	// 事务抢锁
	// 返回成功/失败:释放租约
	response, err := this.lease.Grant(context.TODO(), 5)
	if err != nil {
		return err
	}

	// 用于取消自动续租
	withCancel, cancelFunc := context.WithCancel(context.TODO())

	// 自动续租
	leaseID := response.ID
	keepAliveResponses, err := this.lease.KeepAlive(withCancel, leaseID)
	if err != nil {
		cancelFunc() // 取消自动续租

		this.lease.Revoke(context.TODO(), leaseID) // 释放租约

		return err
	}

	// 处理续租应答的协程
	go func() {
		for {
			select {
			case <-keepAliveResponses:
				if <-keepAliveResponses == nil {
					goto END
				}
			}
		}
	END:
	}()

	// 创建事务
	txn := this.kv.Txn(context.TODO())

	// 锁路径
	lockKey := common.JOB_LOCK_DIR + this.jobName

	// 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(lockKey))

	// 提交事务
	txnResponse, err := txn.Commit()
	if err != nil {
		cancelFunc() // 取消自动续租

		this.lease.Revoke(context.TODO(), leaseID) // 释放租约

		return err
	}

	if !txnResponse.Succeeded { // 锁被占用
		cancelFunc() // 取消自动续租

		this.lease.Revoke(context.TODO(), leaseID) // 释放租约

		return common.ERR_LOCK_ALRADY_REQUIRE
	}

	// 抢锁成功
	this.leaseID = leaseID
	this.cancelFunc = cancelFunc
	this.isLocked = true

	return nil
}

// 释放锁
func (this *JobLock) Unlock() {
	if this.isLocked {
		this.cancelFunc()
		this.lease.Revoke(context.TODO(), this.leaseID) // 释放租约
	}
}
