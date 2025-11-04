package mvcc

import (
	"math"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	startKey  []byte
	txn       *MvccTxn
	writeIter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).

	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	writeIter.Seek(EncodeKey(startKey, math.MaxUint64))
	return &Scanner{
		startKey:  startKey,
		txn:       txn,
		writeIter: writeIter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.writeIter.Close()
	scan.txn.Reader.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for scan.writeIter.Valid() {
		item := scan.writeIter.Item()
		commitTS := decodeTimestamp(item.Key())
		userKey := DecodeUserKey(item.Key())

		if commitTS > scan.txn.StartTS {
			// This write happens after my start ts, skip
			// println("user key:", userKey[len(userKey)-1], "commit ts:", commitTS, "start ts:", scan.txn.StartTS)
			scan.writeIter.Next()
			continue
		}

		val, err := item.Value()
		if err != nil {
			return nil, nil, err
		}

		// decode write, find the start ts
		write, err := ParseWrite(val)
		if err != nil {
			return nil, nil, err
		}

		// found := false
		var resultVal []byte

		if write.Kind == WriteKindPut {
			// println("fetching value for key:", userKey[len(userKey)-1])
			value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
			if err != nil {
				return nil, nil, err
			}

			if value == nil {
				panic("key should be found")
			}

			resultVal = append(resultVal, value...)
			scan.writeIter.Seek(EncodeKey(userKey, 0))

			return userKey, resultVal, nil
		} else if write.Kind == WriteKindDelete {
			// println("found a deleted key:", userKey[len(userKey)-1], "at start ts", write.StartTS)
			scan.writeIter.Seek(EncodeKey(userKey, 0))
		} else {
			panic("not implemented")
			// scan.writeIter.Next()
		}
	}
	return nil, nil, nil
}
