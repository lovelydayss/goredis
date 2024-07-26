package config

import (
	"os"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"gopkg.in/yaml.v3"
)

// GlobalConfig 全局配置
type GlobalConfig struct {
	Address string    `yaml:"address"` // 绑定地址
	AOF     AOFConfig `yaml:"aof"`     // aof 配置
}

// AOFConfig aof 配置
type AOFConfig struct {
	IsAOF           bool   `yaml:"is_aof"`               // 是否启用 aof
	FileName        string `yaml:"filename"`             // aof 文件名称
	AppendFsync     string `yaml:"append_fsync"`         // aof 级别
	IsRewrite       bool   `yaml:"is_rewrite"`           // 是否重写 aof
	RewriteInterval int    `yaml:"aof_rewrite_interval"` // 每执行多少次 aof 操作后，进行一次重写
}

// Config 全局配置对象C
var Config = &GlobalConfig{}

func init() {

	file, err := os.Open("./config/config.yaml")
	if err != nil {
		return
	}

	defer file.Close()

	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(Config)
	if err != nil {
		log.Errorf("Error decoding YAML:", err)
		return
	}

}
