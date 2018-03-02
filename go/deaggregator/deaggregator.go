package deaggregator

import (
	"bytes"
	"crypto/md5"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/awslabs/kinesis-aggregation/go/constants"
	"github.com/awslabs/kinesis-aggregation/go/pb.go"
	"github.com/golang/protobuf/proto"
)

func DeaggregateRecords(records []*kinesis.Record) (result []*kinesis.Record, err error) {

	for _, r := range records {

		if isNotAggregated(r) {
			result = append(result, r)
		} else {
			drs, err := deaggregateRecord(r)
			if err != nil {
				return
			}
			result = append(result, drs...)
		}
	}
	return
}

func deaggregateRecord(r *kinesis.Record) (deaggregatedRecords []*kinesis.Record, err error) {

	src := r.Data[len(constants.MagicNumber) : len(r.Data)-md5.Size]
	aggregatedRecord := new(kpl.AggregatedRecord)

	err = proto.Unmarshal(src, aggregatedRecord)
	if err != nil {
		return
	}

	for _, dr := range aggregatedRecord.Records {
		deaggregatedRecords = append(deaggregatedRecords, &kinesis.Record{
			Data:                        dr.GetData(),
			PartitionKey:                &aggregatedRecord.PartitionKeyTable[dr.GetPartitionKeyIndex()],
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: r.ApproximateArrivalTimestamp,
		})
	}
}

// Check if a given entry is aggregated record.
func isNotAggregated(entry *kinesis.Record) bool {
	return bytes.HasPrefix(entry.Data, constants.MagicNumber)
}
