package benchmark

import (
	"os"
	"testing"

	goleveldb "github.com/syndtr/goleveldb/leveldb"
)

func setup_goleveldb(tb testing.TB) *goleveldb.DB {
	db, err := goleveldb.OpenFile("goleveldb.db", nil)
	if err != nil {
		tb.Fatal(err)
	}

	return db
}

func teardown_goleveldb(tb testing.TB, db *goleveldb.DB) {
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := os.RemoveAll("goleveldb.db"); err != nil {
		tb.Fatal(err)
	}
}

func insert_goleveldb(tb testing.TB, db *goleveldb.DB, d []KeyValue) {
	for _, v := range d {
		if err := db.Put(v.Key, v.Value, nil); err != nil {
			tb.Fatal(err)
		}
	}
}

func iterate_goleveldb(tb testing.TB, db *goleveldb.DB) {
	cur := db.NewIterator(nil, nil)
	for cur.Next() {
		_, _ = cur.Key(), cur.Value()
	}
	cur.Release()
	if err := cur.Error(); err != nil {
		tb.Fatal(err)
	}
}

func benchmarkInsert_goleveldb(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_goleveldb(b)
		insert_goleveldb(b, db, Data[:n])
		teardown_goleveldb(b, db)
	}
}

func BenchmarkInsert1_goleveldb(b *testing.B) {
	benchmarkInsert_goleveldb(b, 1)
}

func BenchmarkInsert2_goleveldb(b *testing.B) {
	benchmarkInsert_goleveldb(b, 10)
}

func BenchmarkInsert3_goleveldb(b *testing.B) {
	benchmarkInsert_goleveldb(b, 100)
}

func BenchmarkInsert4_goleveldb(b *testing.B) {
	benchmarkInsert_goleveldb(b, 1000)
}

func BenchmarkInsert5_goleveldb(b *testing.B) {
	benchmarkInsert_goleveldb(b, 10000)
}

func benchmarkInsertSorted_goleveldb(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_goleveldb(b)
		insert_goleveldb(b, db, DataSorted[:n])
		teardown_goleveldb(b, db)
	}
}

func BenchmarkInsertSorted1_goleveldb(b *testing.B) {
	benchmarkInsertSorted_goleveldb(b, 1)
}

func BenchmarkInsertSorted2_goleveldb(b *testing.B) {
	benchmarkInsertSorted_goleveldb(b, 10)
}

func BenchmarkInsertSorted3_goleveldb(b *testing.B) {
	benchmarkInsertSorted_goleveldb(b, 100)
}

func BenchmarkInsertSorted4_goleveldb(b *testing.B) {
	benchmarkInsertSorted_goleveldb(b, 1000)
}

func BenchmarkInsertSorted5_goleveldb(b *testing.B) {
	benchmarkInsertSorted_goleveldb(b, 10000)
}

func benchmarkIterate_goleveldb(b *testing.B, n int) {
	db := setup_goleveldb(b)
	insert_goleveldb(b, db, Data[:n])
	b.ResetTimer()
	defer func() {
		b.StopTimer()
		teardown_goleveldb(b, db)
	}()
	for i := 0; i < b.N; i++ {
		iterate_goleveldb(b, db)
	}
}

func BenchmarkIterate1_goleveldb(b *testing.B) {
	benchmarkIterate_goleveldb(b, 1)
}

func BenchmarkIterate2_goleveldb(b *testing.B) {
	benchmarkIterate_goleveldb(b, 10)
}

func BenchmarkIterate3_goleveldb(b *testing.B) {
	benchmarkIterate_goleveldb(b, 100)
}

func BenchmarkIterate4_goleveldb(b *testing.B) {
	benchmarkIterate_goleveldb(b, 1000)
}

func BenchmarkIterate5_goleveldb(b *testing.B) {
	benchmarkIterate_goleveldb(b, 10000)
}
