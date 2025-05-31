package committee

import (
	"JiangZiya/core"
	"JiangZiya/leader/leader_log"
	"JiangZiya/leader/signal"
	"JiangZiya/message"
	"JiangZiya/network"
	"JiangZiya/params"
	"JiangZiya/partition"
	"JiangZiya/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type JiangZiyaCommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int
	IpNodeTable  map[uint64]map[uint64]string
	leaderLog    *leader_log.LeaderLog
	stopSignal   *signal.StopSignal

	JiangZiyaRunningTime time.Time
	modifiedMap          map[string]uint64
	Freq                 int
	JzyLock              sync.Mutex
	JzyGraph             *partition.JzyState
	curEpoch             int32
}

func NewJiangZiyaCommitteeModule(csvPath string, dataTotalNum int, batchDataNum int, IpNodeTable map[uint64]map[uint64]string, ll *leader_log.LeaderLog, ss *signal.StopSignal, jzyFrequency int) *JiangZiyaCommitteeModule {
	jg := new(partition.JzyState)
	jg.Init_JzyState()
	return &JiangZiyaCommitteeModule{
		csvPath:              csvPath,
		dataTotalNum:         dataTotalNum,
		nowDataNum:           0,
		batchDataNum:         batchDataNum,
		IpNodeTable:          IpNodeTable,
		leaderLog:            ll,
		stopSignal:           ss,
		JiangZiyaRunningTime: time.Time{},
		modifiedMap:          make(map[string]uint64),
		Freq:                 jzyFrequency,
		JzyGraph:             jg,
		curEpoch:             0,
	}
}

func (jcm *JiangZiyaCommitteeModule) MsgSendingControl() {
	// 读取交易数据
	txFile, err := os.Open(jcm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txFile.Close()
	reader := csv.NewReader(txFile)
	txList := make([]*core.Transaction, 0) // save the txs in this epoch
	JiangZiyaCnt := 0
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}

		// 数据转换为交易
		if tx, ok := data2tx(data, uint64(jcm.nowDataNum)); ok {
			txList = append(txList, tx)
			jcm.nowDataNum++
		} else {
			continue
		}

		// 收集到一定数量后 发送
		if len(txList) == int(jcm.batchDataNum) || jcm.nowDataNum == jcm.dataTotalNum {
			if jcm.JiangZiyaRunningTime.IsZero() {
				jcm.JiangZiyaRunningTime = time.Now()
			}

			jcm.txSengding(txList)

			// 发完之后清空txList
			txList = make([]*core.Transaction, 0)
			jcm.stopSignal.StopGap_Reset()
		}

		// 如果算法计时器已开始，并且经过的时间大于等于 JiangZiya 频率
		if !jcm.JiangZiyaRunningTime.IsZero() && time.Since(jcm.JiangZiyaRunningTime) >= time.Duration(jcm.Freq)*time.Second {
			jcm.JzyLock.Lock()
			JiangZiyaCnt++ // 计数

			mmap, _ := jcm.JzyGraph.Jzy_Partition() // 构图+图划分
			jcm.jzyMapSend(mmap)

			// Leader shard本地的map也要更新
			for key, val := range mmap {
				jcm.modifiedMap[key] = val
			}

			jcm.jzyReset()
			jcm.JzyLock.Unlock()

			// 等待当前epoch完成
			for atomic.LoadInt32(&jcm.curEpoch) != int32(JiangZiyaCnt) {
				time.Sleep(time.Second)
			}
			jcm.JiangZiyaRunningTime = time.Now()
			jcm.leaderLog.Llog.Println("Next epoch begins.")
		}

		if jcm.nowDataNum == jcm.dataTotalNum {
			break
		}
	}

	// 所有交易都被发送了 但是图划分还没发送完 继续发送
	for !jcm.stopSignal.GapEnough() {
		time.Sleep(time.Second)
		if time.Since(jcm.JiangZiyaRunningTime) >= time.Duration(jcm.Freq)*time.Second {
			jcm.JzyLock.Lock()
			JiangZiyaCnt++
			mmap, _ := jcm.JzyGraph.Jzy_Partition()
			jcm.jzyMapSend(mmap)
			for key, val := range mmap {
				jcm.modifiedMap[key] = val
			}

			jcm.jzyReset()
			jcm.JzyLock.Unlock()

			for atomic.LoadInt32(&jcm.curEpoch) != int32(JiangZiyaCnt) {
				time.Sleep(time.Second)
			}
			jcm.leaderLog.Llog.Println("Next epoch begins.")
			jcm.JiangZiyaRunningTime = time.Now()
		}
	}
}

func data2tx(data []string, nonce uint64) (*core.Transaction, bool) {
	if data[6] == "0" && data[7] == "0" && len(data[3]) > 16 && len(data[4]) > 16 && data[3] != data[4] {
		val, ok := new(big.Int).SetString(data[8], 10)
		if !ok {
			log.Panic("new big int failed\n")
		}
		tx := core.NewTransaction(data[3][2:], data[4][2:], val, nonce)
		return tx, true
	}

	return &core.Transaction{}, false
}

func (jcm *JiangZiyaCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	jcm.leaderLog.Llog.Printf("Leader Shard: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if atomic.CompareAndSwapInt32(&jcm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		jcm.leaderLog.Llog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}
	jcm.JzyLock.Lock()
	for _, tx := range b.ExcutedTxs {
		jcm.JzyGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Receiver})
	}
	jcm.JzyLock.Unlock()
}

func (jcm *JiangZiyaCommitteeModule) HandleOtherMessage([]byte) {

}

func (jcm *JiangZiyaCommitteeModule) txSengding(txList []*core.Transaction) {
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txList); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txList)) {
			for shardID := uint64(0); shardID < uint64(params.ShardNum); shardID++ {
				it := message.InjectTxs{
					Txs:       sendToShard[shardID],
					ToShardID: shardID,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				sendMsg := message.MergeMessage(message.CInject, itByte)
				go network.TcpDial(sendMsg, jcm.IpNodeTable[shardID][0])
			}

			// reset
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txList) {
			break
		}
		tx := txList[idx]
		senderShardID := jcm.fetchModifiedMap(tx.Sender)
		sendToShard[senderShardID] = append(sendToShard[senderShardID], tx)
	}
}

func (jcm *JiangZiyaCommitteeModule) fetchModifiedMap(key utils.Address) uint64 {
	if val, ok := jcm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (jcm *JiangZiyaCommitteeModule) jzyMapSend(m map[string]uint64) {
	pm := message.PartitionModifiedMap{m}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic(err)
	}

	send_msg := message.MergeMessage(message.CPratitionMsg, pmByte)
	// 发送给E分片的leader节点
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go network.TcpDial(send_msg, jcm.IpNodeTable[i][0])
	}
	jcm.leaderLog.Llog.Println("Leader Shard: all partition map message has been sent.")
}

func (jcm *JiangZiyaCommitteeModule) jzyReset() {
	jcm.JzyGraph = new(partition.JzyState)
	jcm.JzyGraph.Init_JzyState()
	for key, val := range jcm.modifiedMap {
		jcm.JzyGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}
