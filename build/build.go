package build

import (
	"JiangZiya/executor/pbft"
	"JiangZiya/leader"
	"JiangZiya/params"
	"strconv"
	"time"
)

func BuildLeaderShard(nodeNum, shardNum, modID uint64) {
	var measureMod []string
	// 选择测试模式 目前只有一种模式
	if modID == 0 {
		measureMod = params.MeasureRelayMod
	}

	lsn := new(leader.Leader)
	lsn.NewLeader(params.LeaderAddr, initChainConfig(123, nodeNum, 123, shardNum), params.CommitteeMethod[modID], measureMod...)
	time.Sleep(10000 * time.Millisecond)
	go lsn.LeaderTxHandling()
	lsn.TcpListen()
}

func initChainConfig(nodeID, nodeNum, shardID, shardNum uint64) *params.ChainConfig {
	// 创建节点列表
	params.ShardNum = int(shardNum)
	for i := uint64(0); i < shardNum; i++ {
		if _, ok := params.IPmap_nodeTable[i]; !ok {
			params.IPmap_nodeTable[i] = make(map[uint64]string)
		}

		for j := uint64(0); j < nodeNum; j++ {
			params.IPmap_nodeTable[i][j] = "127.0.0.1:" + strconv.Itoa(28800+int(i)*100+int(j))
		}
	}

	// 构建Leader Shard
	params.IPmap_nodeTable[params.LeaderShard] = make(map[uint64]string)
	params.IPmap_nodeTable[params.LeaderShard][0] = params.LeaderAddr

	params.NodesInShard = int(nodeNum)
	params.ShardNum = int(shardNum)

	chainConfig := &params.ChainConfig{
		ChainID:        shardID,
		NodeID:         nodeID,
		ShardID:        shardID,
		Nodes_perShard: uint64(params.NodesInShard),
		ShardNums:      shardNum,
		BlockInterval:  uint64(params.MaxBlockSize_global),
		InjectSpeed:    uint64(params.InjectSpeed),
	}
	return chainConfig
}

func BuildExecutionNode(nid, nnm, sid, snm, mod uint64) {
	worker := pbft.NewPbftNode(sid, nid, initChainConfig(nid, nnm, sid, snm), params.CommitteeMethod[mod])
	go worker.TcpListen()
	worker.Propose()
}
