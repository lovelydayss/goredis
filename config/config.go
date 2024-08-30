package config

import (
	"os"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"gopkg.in/yaml.v3"
)

// GlobalConfig 全局配置
type GlobalConfig struct {
	Server  ServerConfig  `yaml:"server"`  // 服务器配置
	AOF     AOFConfig     `yaml:"aof"`     // aof 配置
	Cluster ClusterConfig `yaml:"cluster"` // 集群配置
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Address string `yaml:"address"` // 绑定地址
}

// AOFConfig aof 配置
type AOFConfig struct {
	IsAOF           bool   `yaml:"is_enable"`            // 是否启用 aof
	FileName        string `yaml:"filename"`             // aof 文件名称
	AppendFsync     string `yaml:"append_fsync"`         // aof 级别
	IsRewrite       bool   `yaml:"is_rewrite"`           // 是否重写 aof
	RewriteInterval int    `yaml:"aof_rewrite_interval"` // 每执行多少次 aof 操作后，进行一次重写
}

// ClusterConfig 集群配置
type ClusterConfig struct {
	IsEnabled    bool    `yaml:"is_enabled"`    // 是否启用集群
	PartitionNum int     `yaml:"partition_num"` // 分区数
	PartitionMap [][]int `yaml:"partition_map"` // 分区映射，格式：{{begin,end}, ...}}

	RaftNodeNums    int        `yaml:"raft_node_nums"`    // Raft节点数
	RaftNodeAddress [][]string `yaml:"raft_node_address"` // Raft节点地址
}

// Config 全局配置对象
var Config = &GlobalConfig{}

func init() {

	file, err := os.Open("./config.yaml")
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
