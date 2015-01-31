package benchmark

import (
	"bytes"
	"math/rand"
	"sort"
)

var dataBytes = []byte("data")

const Count = 10000

type KeyValue struct {
	Key   []byte
	Value []byte
}
type KeyValueString struct {
	Key   string
	Value []byte
}

var Data = func() [Count]KeyValue {
	var kv [Count]KeyValue

	r := rand.New(rand.NewSource(42))

	randomBytes := func(l int) []byte {
		b := make([]byte, l)
		for i := range b {
			b[i] = byte(r.Intn(256))
		}
		return b
	}

	for i := range kv {
		kv[i].Key = randomBytes(r.Intn(100) + 10)
		kv[i].Value = randomBytes(r.Intn(10000) + 10)
	}

	return kv
}()

var DataString = func() [Count]KeyValueString {
	var kv [Count]KeyValueString

	for i := range kv {
		kv[i].Key = string(Data[i].Key)
		kv[i].Value = Data[i].Value
	}

	return kv
}()

type keyValueSort []KeyValue
type keyValueStringSort []KeyValueString

func (kv keyValueSort) Len() int       { return len(kv) }
func (kv keyValueStringSort) Len() int { return len(kv) }

func (kv keyValueSort) Swap(i, j int)       { kv[i], kv[j] = kv[j], kv[i] }
func (kv keyValueStringSort) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }

func (kv keyValueSort) Less(i, j int) bool       { return bytes.Compare(kv[i].Key, kv[j].Key) < 0 }
func (kv keyValueStringSort) Less(i, j int) bool { return kv[i].Key < kv[j].Key }

var DataSorted = func() [Count]KeyValue {
	kv := Data

	sort.Sort(keyValueSort(kv[:]))

	return kv
}()

var DataSortedString = func() [Count]KeyValueString {
	kv := DataString

	sort.Sort(keyValueStringSort(kv[:]))

	return kv
}()
