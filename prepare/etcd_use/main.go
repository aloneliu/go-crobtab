package main

import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"log"
	"context"
)

func main() {
	config := clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	}

	client, err := clientv3.New(config)
	if err != nil {
		log.Println(err)
	}

	// 申请一个lease (租约) 10s
	response, err := clientv3.NewLease(client).Grant(context.TODO(), 10)
	if err != nil {
		log.Println(err)
	}

	// 租约ID
	leaseID := response.ID

	// 获得kvAPI子集
	kv := clientv3.NewKV(client)

	putResponse, err := kv.Put(context.TODO(), "/cron/lock/job1", "", clientv3.WithLease(leaseID))
	if err != nil {
		log.Println(err)
	}

	log.Println("成功:", putResponse.Header.Revision)

	// 是否过期
	for {
		getResponse, err := kv.Get(context.TODO(), "/cron/lock/job1")
		if err != nil {
			log.Println(err)
		}
		if getResponse.Count == 0 {
			log.Println("kv过期了")
			break
		}
		log.Println("没过期", getResponse.Kvs)

		time.Sleep(2 * time.Second)
	}

}
