package partition

import "JiangZiya/utils"

type JzyState struct {
	CCG               Graph          // 需要运行JZY算法的CCG
	PartitionMap      map[Vertex]int // map[节点]所属分片id
	VertexsNumInShard []int          // Shard 内节点的数目
}

func (js *JzyState) Jzy_Partition() (map[string]uint64, int) {
	return map[string]uint64{}, 0
}

func (js *JzyState) Encode() {

}

func (js *JzyState) Init_JzyState() {
	js.PartitionMap = make(map[Vertex]int)
}

// 加入边，需要将它的端点（如果不存在）默认归到一个分片中
func (js *JzyState) AddEdge(u, v Vertex) {
	// 如果没有点，则增加
	if _, ok := js.CCG.VertexSet[u]; !ok {
		js.AddVertex(u)
	}
	if _, ok := js.CCG.VertexSet[v]; !ok {
		js.AddVertex(v)
	}
	js.CCG.AddEdge(u, v)
}

// 加入节点，规划到一个分片中
func (js *JzyState) AddVertex(u Vertex) {
	js.CCG.AddVertex(u)
	if val, ok := js.PartitionMap[u]; !ok {
		js.PartitionMap[u] = utils.Addr2Shard(u.Addr)
	} else {
		js.PartitionMap[u] = val
	}
	js.VertexsNumInShard[js.PartitionMap[u]] += 1
}
