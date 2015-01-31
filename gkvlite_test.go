package benchmark

import (
	"os"
	"testing"

	"github.com/steveyen/gkvlite"
)

func setup_gkvlite(tb testing.TB) (*os.File, *gkvlite.Store) {
	f, err := os.Create("gkvlite.db")
	if err != nil {
		tb.Fatal(err)
	}
	db, err := gkvlite.NewStore(f)
	if err != nil {
		tb.Fatal(err)
	}

	return f, db
}

func teardown_gkvlite(tb testing.TB, f *os.File, db *gkvlite.Store) {
	if err := db.Flush(); err != nil {
		tb.Fatal(err)
	}
	db.Close()

	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := os.Remove("gkvlite.db"); err != nil {
		tb.Fatal(err)
	}
}

func insert_gkvlite(tb testing.TB, db *gkvlite.Store, d []KeyValue) {
	c := db.SetCollection("data", nil)
	for _, v := range d {
		if err := c.Set(v.Key, v.Value); err != nil {
			tb.Fatal(err)
		}
	}
}

func iterate_gkvlite(tb testing.TB, db *gkvlite.Store) {
	c := db.GetCollection("data")
	if err := c.VisitItemsAscend(nil, true, func(i *gkvlite.Item) bool {
		_, _ = i.Key, i.Val
		return true
	}); err != nil {
		tb.Fatal(err)
	}
}

func benchmarkInsert_gkvlite(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		f, db := setup_gkvlite(b)
		insert_gkvlite(b, db, Data[:n])
		teardown_gkvlite(b, f, db)
	}
}

func BenchmarkInsert1_gkvlite(b *testing.B) {
	benchmarkInsert_gkvlite(b, 1)
}

func BenchmarkInsert2_gkvlite(b *testing.B) {
	benchmarkInsert_gkvlite(b, 10)
}

func BenchmarkInsert3_gkvlite(b *testing.B) {
	benchmarkInsert_gkvlite(b, 100)
}

func BenchmarkInsert4_gkvlite(b *testing.B) {
	benchmarkInsert_gkvlite(b, 1000)
}

func BenchmarkInsert5_gkvlite(b *testing.B) {
	benchmarkInsert_gkvlite(b, 10000)
}

func benchmarkInsertSorted_gkvlite(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		f, db := setup_gkvlite(b)
		insert_gkvlite(b, db, DataSorted[:n])
		teardown_gkvlite(b, f, db)
	}
}

func BenchmarkInsertSorted1_gkvlite(b *testing.B) {
	benchmarkInsertSorted_gkvlite(b, 1)
}

func BenchmarkInsertSorted2_gkvlite(b *testing.B) {
	benchmarkInsertSorted_gkvlite(b, 10)
}

func BenchmarkInsertSorted3_gkvlite(b *testing.B) {
	benchmarkInsertSorted_gkvlite(b, 100)
}

func BenchmarkInsertSorted4_gkvlite(b *testing.B) {
	benchmarkInsertSorted_gkvlite(b, 1000)
}

func BenchmarkInsertSorted5_gkvlite(b *testing.B) {
	benchmarkInsertSorted_gkvlite(b, 10000)
}

func benchmarkIterate_gkvlite(b *testing.B, n int) {
	f, db := setup_gkvlite(b)
	insert_gkvlite(b, db, Data[:n])
	b.ResetTimer()
	defer func() {
		b.StopTimer()
		teardown_gkvlite(b, f, db)
	}()
	for i := 0; i < b.N; i++ {
		iterate_gkvlite(b, db)
	}
}

func BenchmarkIterate1_gkvlite(b *testing.B) {
	benchmarkIterate_gkvlite(b, 1)
}

func BenchmarkIterate2_gkvlite(b *testing.B) {
	benchmarkIterate_gkvlite(b, 10)
}

func BenchmarkIterate3_gkvlite(b *testing.B) {
	benchmarkIterate_gkvlite(b, 100)
}

func BenchmarkIterate4_gkvlite(b *testing.B) {
	benchmarkIterate_gkvlite(b, 1000)
}

func BenchmarkIterate5_gkvlite(b *testing.B) {
	benchmarkIterate_gkvlite(b, 10000)
}
