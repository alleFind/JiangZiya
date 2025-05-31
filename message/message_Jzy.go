package message

import (
	"JiangZiya/core"
	"bytes"
	"encoding/gob"
	"log"
)

var (
	CPratitionMsg       MessageType = "PartitionModifiedMap"
	PartitionReq        RequestType = "PartitionReq"
	CPartitionReady     MessageType = "ready for partition"
	AccountState_and_TX MessageType = "AccountState&txs"
)

type PartitionModifiedMap struct {
	PartitionModified map[string]uint64 // map[账户]所属分片id
}

type AccountStateAndTx struct {
	Addrs        []string
	AccountState []*core.AccountState
	Txs          []*core.Transaction
	FromShard    uint64
}

type AccountTransferMsg struct {
	ModifiedMap  map[string]uint64
	Addrs        []string
	AccountState []*core.AccountState
	ATid         uint64
}

func DecodeAccountTransferMsg(content []byte) *AccountTransferMsg {
	var atm AccountTransferMsg

	decoder := gob.NewDecoder(bytes.NewReader(content))
	err := decoder.Decode(&atm)
	if err != nil {
		log.Panic(err)
	}

	return &atm
}

// if transaction relaying is used, this message is used for sending sequence id, too
type Relay struct {
	Txs           []*core.Transaction
	SenderShardID uint64
	SenderSeq     uint64
}

type PartitionReady struct {
	FromShard uint64
	NowSeqID  uint64
}

func (atm *AccountTransferMsg) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(atm)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}
