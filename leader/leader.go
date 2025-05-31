package leader

import (
	"JiangZiya/leader/committee"
	"JiangZiya/leader/leader_log"
	"JiangZiya/leader/measure"
	"JiangZiya/leader/signal"
	"JiangZiya/message"
	"JiangZiya/network"
	"JiangZiya/params"
	"bufio"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type Leader struct {
	IPaddr       string                       // leader的ip
	ChainConfig  *params.ChainConfig          // 链的基本配置
	Ip_nodeTable map[uint64]map[uint64]string // leader本地维护的节点列表

	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex

	leaderLog *leader_log.LeaderLog

	stopSignal *signal.StopSignal

	comMod committee.CommitteeModule

	testMeasureMods []measure.MeasureModule
}

func (leader *Leader) NewLeader(leaderAddr string, chainConfig *params.ChainConfig, mod string, measureModNames ...string) {
	leader.IPaddr = leaderAddr
	leader.ChainConfig = chainConfig
	leader.Ip_nodeTable = params.IPmap_nodeTable
	leader.leaderLog = leader_log.NewLeaderLog()
	leader.stopSignal = signal.NewStopSignal(2 * int(chainConfig.ShardNums))

	// todo
	switch mod {
	case "JiangZiya":
		leader.comMod = committee.NewJiangZiyaCommitteeModule(params.FileInput, params.TotalDataSize, params.BatchSize, leader.Ip_nodeTable, leader.leaderLog, leader.stopSignal, params.ReconfigTimeGap)
	default:
	}

	leader.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, m := range measureModNames {
		switch m {
		case "TPS_Relay":
			println("This is TPS_Relay measure")
		case "TCL_Relay":
			println("This is TCL_Relay measure")
		case "CrossTxRate_Relay":
			println("This is CrossTxRate_Relay measure")
		case "TxNumberCount_Relay":
			println("This is TxNumberCount_Relay measure")
		default:
		}
	}
}

// Leader分片 L分片负责交易的预执行以获得每轮epoch的交易的情况，即合约之间的调用关系。
// -> 构建合约调用图，采用metis图划分 -> 发送划分结果给对应分片进行合约迁移
func (leader *Leader) LeaderTxHandling() {
	leader.comMod.MsgSendingControl()

	// TxHandling is end
	for !leader.stopSignal.GapEnough() {
		time.Sleep(time.Second)
	}
	// 发送停止消息
	stopMsg := message.MergeMessage(message.CStop, []byte("this is a stop message."))
	leader.leaderLog.Llog.Println("Leader Shard: now sending cstop message to all nodes")
	for shardID := uint64(0); shardID < leader.ChainConfig.ShardNums; shardID++ {
		for nodeID := uint64(0); nodeID < leader.ChainConfig.Nodes_perShard; nodeID++ {
			network.TcpDial(stopMsg, leader.Ip_nodeTable[shardID][nodeID])
		}
	}

	leader.leaderLog.Llog.Println("Leader Shard: now closing...")
	leader.listenStop = true
	leader.CloseLeader()
}

func (leader *Leader) TcpListen() {
	listen, err := net.Listen("tcp", leader.IPaddr)
	if err != nil {
		log.Panic(err)
	}

	leader.tcpLn = listen
	for {
		conn, err := leader.tcpLn.Accept()
		if err != nil {
			return
		}
		go leader.handleClientRequest(conn)
	}
}

func (leader *Leader) CloseLeader() {
	leader.leaderLog.Llog.Println("Closing...")
	for _, measureMod := range leader.testMeasureMods {
		leader.leaderLog.Llog.Println(measureMod.OutputMetricName())
		leader.leaderLog.Llog.Println(measureMod.OutputRecord())
		println()
	}

	leader.leaderLog.Llog.Println("Trying to input .csv")
	// write to .csv file
	dirpath := params.DataWrite_path + "leader_measureOutput/"
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	for _, measureMod := range leader.testMeasureMods {
		targetPath := dirpath + measureMod.OutputMetricName() + ".csv"
		f, err := os.Open(targetPath)
		resultPerEpoch, totResult := measureMod.OutputRecord()
		resultStr := make([]string, 0)
		for _, result := range resultPerEpoch {
			resultStr = append(resultStr, strconv.FormatFloat(result, 'f', 8, 64))
		}
		resultStr = append(resultStr, strconv.FormatFloat(totResult, 'f', 8, 64))
		if err != nil && os.IsNotExist(err) {
			file, er := os.Create(targetPath)
			if er != nil {
				panic(er)
			}
			defer file.Close()

			w := csv.NewWriter(file)
			title := []string{measureMod.OutputMetricName()}
			w.Write(title)
			w.Flush()
			w.Write(resultStr)
			w.Flush()
		} else {
			file, err := os.OpenFile(targetPath, os.O_APPEND|os.O_RDWR, 0666)

			if err != nil {
				log.Panic(err)
			}
			defer file.Close()
			writer := csv.NewWriter(file)
			err = writer.Write(resultStr)
			if err != nil {
				log.Panic()
			}
			writer.Flush()
		}
		f.Close()
		leader.leaderLog.Llog.Println(measureMod.OutputRecord())
	}
	network.CloseAllConnInPool()
	leader.tcpLn.Close()
}

func (leader *Leader) handleClientRequest(conn net.Conn) {
	defer conn.Close()
	clientReader := bufio.NewReader(conn)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			leader.tcpLock.Lock()
			leader.handleMessage(clientRequest)
			leader.tcpLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

// 处理接收到的消息
func (leader *Leader) handleMessage(request []byte) {
	msgType, content := message.SplitMessage(request)
	switch msgType {
	case message.CBlockInfo:
		leader.handleBlockInfos(content)
		// add codes for more functionality
	default:
		leader.comMod.HandleOtherMessage(request)
		for _, mm := range leader.testMeasureMods {
			mm.HandleExtraMessage(request)
		}
	}
}

// Leader Shard接收来自其他分片的leader节点的消息
func (leader *Leader) handleBlockInfos(content []byte) {
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	// StopSignal check
	if bim.BlockBodyLength == 0 {
		leader.stopSignal.StopGap_Increase()
	} else {
		leader.stopSignal.StopGap_Reset()
	}

	leader.comMod.HandleBlockInfo(bim)

	// measure update
	for _, measureMod := range leader.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}
}
