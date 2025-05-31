package measure

import "JiangZiya/message"

type MeasureModule interface {
	UpdateMeasureRecord(*message.BlockInfoMsg)
	OutputMetricName() string
	OutputRecord() ([]float64, float64)
	HandleExtraMessage(request []byte)
}
