package benchmark

import (
	"os"
	"testing"

	leveldb_go "code.google.com/p/leveldb-go/leveldb"
)

func setup_leveldb_go(tb testing.TB) *leveldb_go.DB {
	db, err := leveldb_go.Open("leveldb_go.db", nil)
	if err != nil {
		tb.Fatal(err)
	}

	return db
}

func teardown_leveldb_go(tb testing.TB, db *leveldb_go.DB) {
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := os.RemoveAll("leveldb_go.db"); err != nil {
		tb.Fatal(err)
	}
}

func insert_leveldb_go(tb testing.TB, db *leveldb_go.DB, d []KeyValue) {
	for _, v := range d {
		if err := db.Set(v.Key, v.Value, nil); err != nil {
			tb.Fatal(err)
		}
	}
}

func iterate_leveldb_go(tb testing.TB, db *leveldb_go.DB) {
	defer func() {
		if r := recover(); r == "unimplemented" {
			tb.Skip("leveldb_go does not support iteration.")
		} else if r != nil {
			panic(r)
		}
	}()

	cur := db.Find(nil, nil)
	for cur.Next() {
		_, _ = cur.Key(), cur.Value()
	}
	if err := cur.Close(); err != nil {
		tb.Fatal(err)
	}
}

func get_leveldb_go(tb testing.TB, db *leveldb_go.DB, d []KeyValue) {
	for _, v := range d {
		if _, err := db.Get(v.Key, nil); err != nil {
			tb.Fatal(err)
		}
	}
}

func benchmarkInsert_leveldb_go(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_leveldb_go(b)
		insert_leveldb_go(b, db, Data[:n])
		teardown_leveldb_go(b, db)
	}
}

func BenchmarkInsert1_leveldb_go(b *testing.B) {
	benchmarkInsert_leveldb_go(b, 1)
}

func BenchmarkInsert2_leveldb_go(b *testing.B) {
	benchmarkInsert_leveldb_go(b, 10)
}

func BenchmarkInsert3_leveldb_go(b *testing.B) {
	benchmarkInsert_leveldb_go(b, 100)
}

func BenchmarkInsert4_leveldb_go(b *testing.B) {
	benchmarkInsert_leveldb_go(b, 1000)
}

func BenchmarkInsert5_leveldb_go(b *testing.B) {
	benchmarkInsert_leveldb_go(b, 10000)
}

func benchmarkInsertSorted_leveldb_go(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_leveldb_go(b)
		insert_leveldb_go(b, db, DataSorted[:n])
		teardown_leveldb_go(b, db)
	}
}

func BenchmarkInsertSorted1_leveldb_go(b *testing.B) {
	benchmarkInsertSorted_leveldb_go(b, 1)
}

func BenchmarkInsertSorted2_leveldb_go(b *testing.B) {
	benchmarkInsertSorted_leveldb_go(b, 10)
}

func BenchmarkInsertSorted3_leveldb_go(b *testing.B) {
	benchmarkInsertSorted_leveldb_go(b, 100)
}

func BenchmarkInsertSorted4_leveldb_go(b *testing.B) {
	benchmarkInsertSorted_leveldb_go(b, 1000)
}

func BenchmarkInsertSorted5_leveldb_go(b *testing.B) {
	benchmarkInsertSorted_leveldb_go(b, 10000)
}

func benchmarkIterate_leveldb_go(b *testing.B, n int) {
	db := setup_leveldb_go(b)
	insert_leveldb_go(b, db, Data[:n])
	b.ResetTimer()
	defer func() {
		b.StopTimer()
		teardown_leveldb_go(b, db)
	}()
	for i := 0; i < b.N; i++ {
		iterate_leveldb_go(b, db)
	}
}

func BenchmarkIterate1_leveldb_go(b *testing.B) {
	benchmarkIterate_leveldb_go(b, 1)
}

func BenchmarkIterate2_leveldb_go(b *testing.B) {
	benchmarkIterate_leveldb_go(b, 10)
}

func BenchmarkIterate3_leveldb_go(b *testing.B) {
	benchmarkIterate_leveldb_go(b, 100)
}

func BenchmarkIterate4_leveldb_go(b *testing.B) {
	benchmarkIterate_leveldb_go(b, 1000)
}

func BenchmarkIterate5_leveldb_go(b *testing.B) {
	benchmarkIterate_leveldb_go(b, 10000)
}

func benchmarkGet_leveldb_go(b *testing.B, n int) {
	db := setup_leveldb_go(b)
	insert_leveldb_go(b, db, Data[:n])
	b.ResetTimer()
	defer func() {
		b.StopTimer()
		teardown_leveldb_go(b, db)
	}()
	for i := 0; i < b.N; i++ {
		get_leveldb_go(b, db, Data[:n])
	}
}

func BenchmarkGet1_leveldb_go(b *testing.B) {
	benchmarkGet_leveldb_go(b, 1)
}

func BenchmarkGet2_leveldb_go(b *testing.B) {
	benchmarkGet_leveldb_go(b, 10)
}

func BenchmarkGet3_leveldb_go(b *testing.B) {
	benchmarkGet_leveldb_go(b, 100)
}

func BenchmarkGet4_leveldb_go(b *testing.B) {
	benchmarkGet_leveldb_go(b, 1000)
}

func BenchmarkGet5_leveldb_go(b *testing.B) {
	benchmarkGet_leveldb_go(b, 10000)
}
