package benchmark

import (
	"os"
	"testing"

	"github.com/boltdb/bolt"
)

func setup_bolt(tb testing.TB) *bolt.DB {
	db, err := bolt.Open("bolt.db", 0600, nil)
	if err != nil {
		tb.Fatal(err)
	}
	return db
}

func teardown_bolt(tb testing.TB, db *bolt.DB) {
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := os.Remove("bolt.db"); err != nil {
		tb.Fatal(err)
	}
}

func insert_bolt(tb testing.TB, db *bolt.DB, d []KeyValue) {
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket(dataBytes)
		if err != nil {
			return err
		}
		for _, v := range d {
			bucket.Put(v.Key, v.Value)
		}
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
}

func iterate_bolt(tb testing.TB, db *bolt.DB) {
	if err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(dataBytes)
		cur := bucket.Cursor()

		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			_, _ = k, v
		}

		return nil
	}); err != nil {
		tb.Fatal(err)
	}
}

func benchmarkInsert_bolt(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_bolt(b)
		insert_bolt(b, db, Data[:n])
		teardown_bolt(b, db)
	}
}

func BenchmarkInsert1_bolt(b *testing.B) {
	benchmarkInsert_bolt(b, 1)
}

func BenchmarkInsert2_bolt(b *testing.B) {
	benchmarkInsert_bolt(b, 10)
}

func BenchmarkInsert3_bolt(b *testing.B) {
	benchmarkInsert_bolt(b, 100)
}

func BenchmarkInsert4_bolt(b *testing.B) {
	benchmarkInsert_bolt(b, 1000)
}

func BenchmarkInsert5_bolt(b *testing.B) {
	benchmarkInsert_bolt(b, 10000)
}

func benchmarkInsertSorted_bolt(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		db := setup_bolt(b)
		insert_bolt(b, db, DataSorted[:n])
		teardown_bolt(b, db)
	}
}

func BenchmarkInsertSorted1_bolt(b *testing.B) {
	benchmarkInsertSorted_bolt(b, 1)
}

func BenchmarkInsertSorted2_bolt(b *testing.B) {
	benchmarkInsertSorted_bolt(b, 10)
}

func BenchmarkInsertSorted3_bolt(b *testing.B) {
	benchmarkInsertSorted_bolt(b, 100)
}

func BenchmarkInsertSorted4_bolt(b *testing.B) {
	benchmarkInsertSorted_bolt(b, 1000)
}

func BenchmarkInsertSorted5_bolt(b *testing.B) {
	benchmarkInsertSorted_bolt(b, 10000)
}

func benchmarkIterate_bolt(b *testing.B, n int) {
	db := setup_bolt(b)
	insert_bolt(b, db, Data[:n])
	b.ResetTimer()
	defer func() {
		b.StopTimer()
		teardown_bolt(b, db)
	}()
	for i := 0; i < b.N; i++ {
		iterate_bolt(b, db)
	}
}

func BenchmarkIterate1_bolt(b *testing.B) {
	benchmarkIterate_bolt(b, 1)
}

func BenchmarkIterate2_bolt(b *testing.B) {
	benchmarkIterate_bolt(b, 10)
}

func BenchmarkIterate3_bolt(b *testing.B) {
	benchmarkIterate_bolt(b, 100)
}

func BenchmarkIterate4_bolt(b *testing.B) {
	benchmarkIterate_bolt(b, 1000)
}

func BenchmarkIterate5_bolt(b *testing.B) {
	benchmarkIterate_bolt(b, 10000)
}
