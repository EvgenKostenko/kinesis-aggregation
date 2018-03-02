package aggregator

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/awslabs/kinesis-aggregation/go/constants"
	"github.com/awslabs/kinesis-aggregation/go/pb.go"
)

type Aggregator struct {
	buf    []*kpl.Record
	pkeys  []string
	nbytes int
}

// Size return how many bytes stored in the aggregator.
// including partition keys.
func (a *Aggregator) Size() int {
	return a.nbytes
}

// Count return how many records stored in the aggregator.
func (a *Aggregator) Count() int {
	return len(a.buf)
}

func (a *Aggregator) clear() {
	a.buf = make([]*kpl.Record, 0)
	a.pkeys = make([]string, 0)
	a.nbytes = 0
}
