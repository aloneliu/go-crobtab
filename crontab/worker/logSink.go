package worker

import (
	"github.com/mongodb/mongo-go-driver/mongo"
	"go-crontab-work/crontab/common"
	"context"
	"time"
)

type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var G_log_sink *LogSink

func (this *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch // 超时批次
	)

	for {
		select {
		case log = <-this.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				commitTimer = time.AfterFunc(
					time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							this.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}

			logBatch.Logs = append(logBatch.Logs, log)

			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				this.saveLog(logBatch)

				// 清空logBatch
				logBatch = nil
				// 取消定时器
				commitTimer.Stop()
			}

		case timeoutBatch = <-this.autoCommitChan: // 过期的批次
			// 可能timer stop不掉
			if timeoutBatch != logBatch {
				continue // 跳过已经提交的批次
			}

			// 写入到mongodb
			this.saveLog(timeoutBatch)
			logBatch = nil
		}
	}
}

// 批量写入日志
func (this *LogSink) saveLog(batch *common.LogBatch) {
	this.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 发送日志
func (this *LogSink) Append(log *common.JobLog) {
	select {
	case this.logChan <- log:
	default:
	}
}

func InitLogSink() error {
	client, err := mongo.Connect(context.TODO(), G_config.MongodbUri)

	if err != nil {
		return err
	}

	// 选择DB和collect
	G_log_sink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	// 启动一个处理协程
	go G_log_sink.writeLoop()

	return nil
}
