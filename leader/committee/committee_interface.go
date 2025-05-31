package committee

import "JiangZiya/message"

type CommitteeModule interface {
	HandleBlockInfo(*message.BlockInfoMsg)
	MsgSendingControl()
	HandleOtherMessage([]byte)
}
