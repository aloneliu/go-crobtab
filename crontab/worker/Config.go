package worker

import (
	"io/ioutil"
	"encoding/json"
)

type Config struct {
	EtcdEndPoint        []string `json:"etcdEndPoint"`
	EtcdTimeout         int      `json:"etcdTimeout"`
	MongodbUri          string   `json:"mongodbUri"`
	MongodbConTimeout   int      `json:"mongodbConTimeout"`
	JobLogBatchSize     int      `json:"jobLogBatchSize"`
	JobLogCommitTimeout int      `json:"jobLogCommitTimeout"`
}

// 单例
var (
	G_config *Config
)

// 配置加载
func InitConfig(filename string) error {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	var config Config
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return err
	}

	G_config = &config

	return nil
}
