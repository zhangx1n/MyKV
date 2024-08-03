package xkv

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/zhangx1n/xkv/utils"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func runXkvTest(t *testing.T, opts *Options, test func(t *testing.T, db *DB)) {
	dir, err := ioutil.TempDir("", "xkv-test")
	require.NoError(t, err)
	defer removeDir(dir)
	if opts == nil {
		opts = new(Options)
		opts = getTestOptions(dir)
	} else {
		opts.WorkDir = dir
		opts.DetectConflicts = true

	}

	db := Open(opts)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	test(t, db)
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("Error while removing dir: %v\n", err)
	}
}

func getTestOptions(dir string) *Options {
	opt = &Options{
		WorkDir:          dir,
		SSTableMaxSz:     1 << 10,
		MemTableSize:     1 << 10,
		ValueLogFileSize: 1 << 20,
		ValueThreshold:   0,
		MaxBatchCount:    20,
		MaxBatchSize:     1 << 20,
		DetectConflicts:  true,
	}
	return opt
}

func TestTxnSimple(t *testing.T) {
	runXkvTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransaction(true)

		for i := 0; i < 10; i++ {
			k := []byte(fmt.Sprintf("key=%d", i))
			v := []byte(fmt.Sprintf("val=%d", i))
			require.NoError(t, txn.SetEntry(utils.NewEntry(k, v)))
		}

		item, err := txn.Get([]byte("key=8"))
		require.NoError(t, err)

		require.Equal(t, "val=8", string(item.Entry().Value))
		require.NoError(t, txn.Commit())
	})
}

func TestTxnReadAfterWrite(t *testing.T) {
	test := func(t *testing.T, db *DB) {
		var wg sync.WaitGroup
		N := 100
		wg.Add(N)
		for i := 0; i < N; i++ {
			go func(i int) {
				defer wg.Done()
				key := []byte(fmt.Sprintf("key%d", i))
				err := db.Update(func(tx *Txn) error {
					return tx.SetEntry(utils.NewEntry(key, key))
				})
				require.NoError(t, err)
				err = db.View(func(tx *Txn) error {
					item, err := tx.Get(key)
					require.NoError(t, err)
					require.NoError(t, err)
					require.Equal(t, key, item.e.Key)
					return nil
				})
				require.NoError(t, err)
			}(i)
		}
		wg.Wait()
	}
	t.Run("disk mode", func(t *testing.T) {
		runXkvTest(t, nil, func(t *testing.T, db *DB) {
			test(t, db)
		})
	})
}

/******
func TestTxnVersions(t *testing.T) {
	runXkvTest(t, nil, func(t *testing.T, db *DB) {
		k := []byte("key")
		for i := 1; i < 10; i++ {
			txn := db.NewTransaction(true)

			require.NoError(t, txn.SetEntry(utils.NewEntry(k, []byte(fmt.Sprintf("valversion=%d", i)))))
			require.NoError(t, txn.Commit())
			require.Equal(t, uint64(i), db.orc.readTs())
		}

		checkIterator := func(itr utils.Iterator, i int) {
			defer itr.Close()
			count := 0
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				require.Equal(t, k, item.Entry().Key)

				val := item.Entry().Value
				exp := fmt.Sprintf("valversion=%d", i)
				require.Equal(t, exp, string(val), "i=%d", i)
				count++
			}
			require.Equal(t, 1, count, "i=%d", i) // Should only loop once.
		}

		checkAllVersions := func(itr utils.Iterator, i int) {
			version := uint64(i)


			count := 0
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				require.Equal(t, k, item.Entry().Key)
				require.Equal(t, version, item.Entry().Version)

				val := item.Entry().Value
				exp := fmt.Sprintf("valversion=%d", version)
				require.Equal(t, exp, string(val), "v=%d", version)
				count++

				version--

			}
			require.Equal(t, i, count, "i=%d", i) // Should loop as many times as i.
		}

		for i := 1; i < 10; i++ {
			txn := db.NewTransaction(true)
			txn.readTs = uint64(i) // Read version at i.

			item, err := txn.Get(k)
			require.NoError(t, err)

			val := item.Entry().Value
			require.Equal(t, []byte(fmt.Sprintf("valversion=%d", i)), val,
				"Expected versions to match up at i=%d", i)

			// Try retrieving the latest version forward and reverse.
			itr := txn.NewIterator(DefaultIteratorOptions)
			checkIterator(itr, i)

			opt := DefaultIteratorOptions
			opt.Reverse = true
			itr = txn.NewIterator(opt)
			checkIterator(itr, i)

			// Now try retrieving all versions forward and reverse.
			opt = DefaultIteratorOptions
			opt.AllVersions = true
			itr = txn.NewIterator(opt)
			checkAllVersions(itr, i)
			itr.Close()

			opt = DefaultIteratorOptions
			opt.AllVersions = true
			opt.Reverse = true
			itr = txn.NewIterator(opt)
			checkAllVersions(itr, i)
			itr.Close()

			txn.Discard()
		}
		txn := db.NewTransaction(true)
		defer txn.Discard()
		item, err := txn.Get(k)
		require.NoError(t, err)

		val, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("valversion=9"), val)
	})
}
****/

func TestTxnWriteSkew(t *testing.T) {
	runXkvTest(t, nil, func(t *testing.T, db *DB) {
		// Accounts
		ax := []byte("x")
		ay := []byte("y")

		// Set balance to $100 in each account.
		txn := db.NewTransaction(true)
		defer txn.Discard()
		val := []byte(strconv.Itoa(100))
		require.NoError(t, txn.SetEntry(utils.NewEntry(ax, val)))
		require.NoError(t, txn.SetEntry(utils.NewEntry(ay, val)))
		require.NoError(t, txn.Commit())
		require.Equal(t, uint64(1), db.orc.readTs())

		getBal := func(txn *Txn, key []byte) (bal int) {
			item, err := txn.Get(key)
			require.NoError(t, err)

			val := item.Entry().Value
			bal, err = strconv.Atoi(string(val))
			require.NoError(t, err)
			return bal
		}

		// Start two transactions, each would read both accounts and deduct from one account.
		txn1 := db.NewTransaction(true)

		sum := getBal(txn1, ax)
		sum += getBal(txn1, ay)
		require.Equal(t, 200, sum)
		require.NoError(t, txn1.SetEntry(utils.NewEntry(ax, []byte("0")))) // Deduct 100 from ax.

		// Let's read this back.
		sum = getBal(txn1, ax)
		require.Equal(t, 0, sum)
		sum += getBal(txn1, ay)
		require.Equal(t, 100, sum)
		// Don't commit yet.

		txn2 := db.NewTransaction(true)

		sum = getBal(txn2, ax)
		sum += getBal(txn2, ay)
		require.Equal(t, 200, sum)
		require.NoError(t, txn2.SetEntry(utils.NewEntry(ay, []byte("0")))) // Deduct 100 from ay.

		// Let's read this back.
		sum = getBal(txn2, ax)
		require.Equal(t, 100, sum)
		sum += getBal(txn2, ay)
		require.Equal(t, 100, sum)

		// Commit both now.
		require.NoError(t, txn1.Commit())
		require.Error(t, txn2.Commit()) // This should fail.

		require.Equal(t, uint64(2), db.orc.readTs())
	})
}

func TestConflict(t *testing.T) {
	key := []byte("foo")
	setCount := uint32(0)

	var DefaultIteratorOptions = IteratorOptions{
		Reverse:     false,
		AllVersions: false,
	}

	testAndSet := func(wg *sync.WaitGroup, db *DB) {
		defer wg.Done()
		txn := db.NewTransaction(true)
		defer txn.Discard()

		_, err := txn.Get(key)
		if err == utils.ErrKeyNotFound {
			// Unset the error.
			err = nil
			require.NoError(t, txn.Set(key, []byte("AA")))
			txn.CommitWith(func(err error) {
				if err == nil {
					require.LessOrEqual(t, uint32(1), atomic.AddUint32(&setCount, 1))
				} else {
					require.Error(t, err, utils.ErrConflict)
				}
			})
		}
		require.NoError(t, err)
	}

	testAndSetItr := func(wg *sync.WaitGroup, db *DB) {
		defer wg.Done()
		txn := db.NewTransaction(true)
		defer txn.Discard()

		iopt := DefaultIteratorOptions
		it := txn.NewIterator(iopt)

		found := false
		for it.Seek(key); it.Valid(); it.Next() {
			found = true
		}
		it.Close()

		if !found {
			require.NoError(t, txn.Set(key, []byte("AA")))
			txn.CommitWith(func(err error) {
				if err == nil {
					require.LessOrEqual(t, atomic.AddUint32(&setCount, 1), uint32(1))
				} else {
					require.Error(t, err, utils.ErrConflict)
				}
			})
		}
	}

	runTest := func(t *testing.T, fn func(wg *sync.WaitGroup, db *DB)) {
		loop := 10
		numGo := 16 // This many concurrent transactions.
		for i := 0; i < loop; i++ {
			var wg sync.WaitGroup
			wg.Add(numGo)
			setCount = 0
			runXkvTest(t, nil, func(t *testing.T, db *DB) {
				for j := 0; j < numGo; j++ {
					go fn(&wg, db)
				}
				wg.Wait()
			})
			require.Equal(t, uint32(1), atomic.LoadUint32(&setCount))
		}
	}

	t.Run("TxnGet", func(t *testing.T) {
		runTest(t, testAndSet)
	})

	t.Run("ItrSeek", func(t *testing.T) {
		runTest(t, testAndSetItr)
	})
}
