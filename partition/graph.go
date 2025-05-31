package partition

type Vertex struct {
	Addr string // 账户地址
}
type Graph struct {
	VertexSet map[Vertex]bool
	EdgeSet   map[Vertex][]Vertex
}

// 创建节点
func (v *Vertex) ConstructVertex(s string) {
	v.Addr = s
}

func (g *Graph) AddVertex(v Vertex) {
	if g.VertexSet == nil {
		g.VertexSet = make(map[Vertex]bool)
	}
	g.VertexSet[v] = true
}

func (g *Graph) AddEdge(u Vertex, v Vertex) {
	if _, ok := g.VertexSet[u]; !ok {
		g.AddVertex(u)
	}
	if _, ok := g.VertexSet[v]; !ok {
		g.AddVertex(v)
	}
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex][]Vertex)
	}

	g.EdgeSet[u] = append(g.EdgeSet[u], v)
	g.EdgeSet[v] = append(g.EdgeSet[v], u)
}
