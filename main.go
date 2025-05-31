package main

import (
	"JiangZiya/build"
	"github.com/spf13/pflag"
)

var (
	shardNum int
	nodeNum  int
	shardID  int
	nodeID   int
	modID    int
	isLeader bool
)

func main() {
	pflag.IntVarP(&shardNum, "shardNum", "S", 2, "how many shards are deployed")
	pflag.IntVarP(&nodeNum, "nodeNum", "N", 4, "each shard has N nodes")
	pflag.IntVarP(&shardID, "shardID", "s", 0, "id of the shard to which this node belongs")
	pflag.IntVarP(&nodeID, "nodeID", "n", 0, "id of this node")
	pflag.IntVarP(&modID, "modID", "m", 0, "id of mod")
	pflag.BoolVarP(&isLeader, "Leader", "L", false, "whether this node is a Leader")
	if isLeader {
		build.BuildLeaderShard(uint64(nodeNum), uint64(shardNum), uint64(modID))
	} else {
		build.BuildExecutionNode(uint64(nodeID), uint64(nodeNum), uint64(shardID), uint64(shardNum), uint64(modID))
	}
}
