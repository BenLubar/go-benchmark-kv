package benchmark

import (
	"os"
	"testing"

	"code.google.com/p/gocask"
)

func setup_gocask(tb testing.TB) *gocask.Gocask {
	db, err := gocask.NewGocask("gocask.db")
	if err != nil {
		tb.Fatal(err)
	}

	return db
}

func teardown_gocask(tb testing.TB, db *gocask.Gocask) {
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := os.RemoveAll("gocask.db"); err != nil {
		tb.Fatal(err)
	}
}

func insert_gocask(tb testing.TB, db *gocask.Gocask, d []KeyValueString) {
	for _, v := range d {
		if err := db.Put(v.Key, v.Value); err != nil {
			tb.Fatal(err)
		}
	}
}

func get_gocask(tb testing.TB, db *gocask.Gocask, d []KeyValueString) {
	for _, v := range d {
		if _, err := db.Get(v.Key); err != nil {
			tb.Fatal(err)
		}
	}
}

func benchmarkInsert_gocask(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_gocask(b)
		insert_gocask(b, db, DataString[:n])
		teardown_gocask(b, db)
	}
}

func BenchmarkInsert1_gocask(b *testing.B) {
	benchmarkInsert_gocask(b, 1)
}

func BenchmarkInsert2_gocask(b *testing.B) {
	benchmarkInsert_gocask(b, 10)
}

func BenchmarkInsert3_gocask(b *testing.B) {
	benchmarkInsert_gocask(b, 100)
}

func BenchmarkInsert4_gocask(b *testing.B) {
	benchmarkInsert_gocask(b, 1000)
}

func BenchmarkInsert5_gocask(b *testing.B) {
	benchmarkInsert_gocask(b, 10000)
}

func benchmarkInsertSorted_gocask(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_gocask(b)
		insert_gocask(b, db, DataSortedString[:n])
		teardown_gocask(b, db)
	}
}

func BenchmarkInsertSorted1_gocask(b *testing.B) {
	benchmarkInsertSorted_gocask(b, 1)
}

func BenchmarkInsertSorted2_gocask(b *testing.B) {
	benchmarkInsertSorted_gocask(b, 10)
}

func BenchmarkInsertSorted3_gocask(b *testing.B) {
	benchmarkInsertSorted_gocask(b, 100)
}

func BenchmarkInsertSorted4_gocask(b *testing.B) {
	benchmarkInsertSorted_gocask(b, 1000)
}

func BenchmarkInsertSorted5_gocask(b *testing.B) {
	benchmarkInsertSorted_gocask(b, 10000)
}

func benchmarkIterate_gocask(b *testing.B, n int) {
	b.Skip("gocask does not support iteration.")
}

func BenchmarkIterate1_gocask(b *testing.B) {
	benchmarkIterate_gocask(b, 1)
}

func BenchmarkIterate2_gocask(b *testing.B) {
	benchmarkIterate_gocask(b, 10)
}

func BenchmarkIterate3_gocask(b *testing.B) {
	benchmarkIterate_gocask(b, 100)
}

func BenchmarkIterate4_gocask(b *testing.B) {
	benchmarkIterate_gocask(b, 1000)
}

func BenchmarkIterate5_gocask(b *testing.B) {
	benchmarkIterate_gocask(b, 10000)
}

func benchmarkGet_gocask(b *testing.B, n int) {
	db := setup_gocask(b)
	insert_gocask(b, db, DataString[:n])
	b.ResetTimer()
	defer func() {
		b.StopTimer()
		teardown_gocask(b, db)
	}()
	for i := 0; i < b.N; i++ {
		get_gocask(b, db, DataString[:n])
	}
}

func BenchmarkGet1_gocask(b *testing.B) {
	benchmarkGet_gocask(b, 1)
}

func BenchmarkGet2_gocask(b *testing.B) {
	benchmarkGet_gocask(b, 10)
}

func BenchmarkGet3_gocask(b *testing.B) {
	benchmarkGet_gocask(b, 100)
}

func BenchmarkGet4_gocask(b *testing.B) {
	benchmarkGet_gocask(b, 1000)
}

func BenchmarkGet5_gocask(b *testing.B) {
	benchmarkGet_gocask(b, 10000)
}
