package benchmark

import (
	"io"
	"os"
	"runtime"
	"testing"

	cznic_kv "github.com/cznic/kv"
)

func setup_cznic_kv(tb testing.TB) *cznic_kv.DB {
	n := runtime.GOMAXPROCS(0)

	db, err := cznic_kv.Create("cznic_kv.db", &cznic_kv.Options{})
	if err != nil {
		tb.Fatal(err)
	}

	runtime.GOMAXPROCS(n)

	return db
}

func teardown_cznic_kv(tb testing.TB, db *cznic_kv.DB) {
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := os.Remove("cznic_kv.db"); err != nil {
		tb.Fatal(err)
	}
}

func insert_cznic_kv(tb testing.TB, db *cznic_kv.DB, d []KeyValue) {
	for _, v := range d {
		if err := db.Set(v.Key, v.Value); err != nil {
			tb.Fatal(err)
		}
	}
}

func iterate_cznic_kv(tb testing.TB, db *cznic_kv.DB) {
	cur, err := db.SeekFirst()
	for err == nil {
		_, _, err = cur.Next()
	}
	if err == io.EOF {
		return
	}
	tb.Fatal(err)
}

func benchmarkInsert_cznic_kv(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_cznic_kv(b)
		insert_cznic_kv(b, db, Data[:n])
		teardown_cznic_kv(b, db)
	}
}

func BenchmarkInsert1_cznic_kv(b *testing.B) {
	benchmarkInsert_cznic_kv(b, 1)
}

func BenchmarkInsert2_cznic_kv(b *testing.B) {
	benchmarkInsert_cznic_kv(b, 10)
}

func BenchmarkInsert3_cznic_kv(b *testing.B) {
	benchmarkInsert_cznic_kv(b, 100)
}

func BenchmarkInsert4_cznic_kv(b *testing.B) {
	benchmarkInsert_cznic_kv(b, 1000)
}

func BenchmarkInsert5_cznic_kv(b *testing.B) {
	benchmarkInsert_cznic_kv(b, 10000)
}

func benchmarkInsertSorted_cznic_kv(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_cznic_kv(b)
		insert_cznic_kv(b, db, DataSorted[:n])
		teardown_cznic_kv(b, db)
	}
}

func BenchmarkInsertSorted1_cznic_kv(b *testing.B) {
	benchmarkInsertSorted_cznic_kv(b, 1)
}

func BenchmarkInsertSorted2_cznic_kv(b *testing.B) {
	benchmarkInsertSorted_cznic_kv(b, 10)
}

func BenchmarkInsertSorted3_cznic_kv(b *testing.B) {
	benchmarkInsertSorted_cznic_kv(b, 100)
}

func BenchmarkInsertSorted4_cznic_kv(b *testing.B) {
	benchmarkInsertSorted_cznic_kv(b, 1000)
}

func BenchmarkInsertSorted5_cznic_kv(b *testing.B) {
	benchmarkInsertSorted_cznic_kv(b, 10000)
}

func benchmarkIterate_cznic_kv(b *testing.B, n int) {
	db := setup_cznic_kv(b)
	insert_cznic_kv(b, db, Data[:n])
	b.ResetTimer()
	defer func() {
		b.StopTimer()
		teardown_cznic_kv(b, db)
	}()
	for i := 0; i < b.N; i++ {
		iterate_cznic_kv(b, db)
	}
}

func BenchmarkIterate1_cznic_kv(b *testing.B) {
	benchmarkIterate_cznic_kv(b, 1)
}

func BenchmarkIterate2_cznic_kv(b *testing.B) {
	benchmarkIterate_cznic_kv(b, 10)
}

func BenchmarkIterate3_cznic_kv(b *testing.B) {
	benchmarkIterate_cznic_kv(b, 100)
}

func BenchmarkIterate4_cznic_kv(b *testing.B) {
	benchmarkIterate_cznic_kv(b, 1000)
}

func BenchmarkIterate5_cznic_kv(b *testing.B) {
	benchmarkIterate_cznic_kv(b, 10000)
}
