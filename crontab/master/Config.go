package master

import (
	"io/ioutil"
	"encoding/json"
)

type Config struct {
	Port         int      `json:"port"`
	ReadTimeout  int      `json:"readTimeout"`
	WriteTimeout int      `json:"writeTimeout"`
	EtcdEndPoint []string `json:"etcdEndPoint"`
	EtcdTimeout  int      `json:"etcdTimeout"`
	WebRoot      string   `json:"webRoot"`
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
