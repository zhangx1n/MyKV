package xkv

import (
	"bytes"
	"context"
	"encoding/hex"
	"github.com/pkg/errors"
	"github.com/zhangx1n/xkv/utils"
	"sort"
	"sync"
	"sync/atomic"
)

type oracle struct {
	detectConflicts bool // Determines if the txns should be checked for conflicts.

	sync.Mutex // For nextTxnTs and commits.
	// writeChLock lock is for ensuring that transactions go to the write
	// channel in the same order as their commit timestamps.
	writeChLock sync.Mutex
	nextTxnTs   uint64

	// Used to block NewTransaction, so all previous commits ars visible to a new read.
	txnMark *utils.WaterMark

	// Either of these is used to determine which versions can be permanently
	// discarded during compaction.
	discardTs uint64           // Used by ManagedDB.
	readMark  *utils.WaterMark // Used by DB.

	// committedTxns contains all committed writes (contains fingerprints
	// of keys written and their latest commit counter).
	committedTxns []committedTxn
	lastCleanupTs uint64

	// closer is used to stop watermarks.
	closer *utils.Closer
}

type committedTxn struct {
	ts uint64
	// ConflictKeys Keeps track of the entries written at timestamp ts.
	conflictKeys map[uint64]struct{}
}

func newOracle(opt Options) *oracle {
	orc := &oracle{
		detectConflicts: opt.DetectConflicts,
		// We're not initializing nextTxnTs and readOnlyTs. It would be done after replay in Open.
		//
		// WaterMarks must be 64-bit aligned for atomic package, hence we must use pointers here.
		// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
		readMark:  &utils.WaterMark{Name: "xkv.PendingReads"},
		txnMark:   &utils.WaterMark{Name: "xkv.TxnTimestamp"},
		closer:    utils.NewCloserInitial(2),
		nextTxnTs: 1,
	}
	orc.readMark.Init(orc.closer)
	orc.txnMark.Init(orc.closer)
	return orc
}

func (o *oracle) Stop() {
	o.closer.SignalAndWait()
}

func (o *oracle) readTs() uint64 {
	var readTs uint64
	o.Lock()
	readTs = o.nextTxnTs - 1
	o.readMark.Begin(readTs)
	o.Unlock()

	// Wait for all txns which have no conflicts, have been assigned a commit
	// timestamp and are going through the write to value log and LSM tree
	// process. Not waiting here could mean that some txns which have been
	// committed would not be read.
	utils.Check(o.txnMark.WaitForMark(context.Background(), readTs))
	return readTs
}

func (o *oracle) nextTs() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.nextTxnTs
}

func (o *oracle) incrementNextTs() {
	o.Lock()
	defer o.Unlock()
	o.nextTxnTs++
}

// Any deleted or invalid versions at or below ts would be discarded during
// compaction to reclaim disk space in LSM tree and thence value log.
func (o *oracle) setDiscardTs(ts uint64) {
	o.Lock()
	defer o.Unlock()
	o.discardTs = ts
	o.cleanupCommittedTransactions()
}

func (o *oracle) discardAtOrBelow() uint64 {
	return o.readMark.DoneUntil()
}

// hasConflict must be called while having a lock.
// 1. 遍历 oracel 事务管理器中已提交的事务, 如果遍历到的事务结束 ts 小于等于 当前事务的 readTs 则跳过.
// 2. 如果当前事务跟遍历的事务 committedTxn 有时间重叠, 那么需要判断 committedTxn 是否含有传入 txn 的 reads 的 keys.
// 3. 有, 则说明发生了事务冲突.
func (o *oracle) hasConflict(txn *Txn) bool {
	// 读事务列表为空
	if len(txn.reads) == 0 {
		return false
	}
	// 遍历 oracle 存在的已提交事务
	for _, committedTxn := range o.committedTxns {
		// If the committedTxn.ts is less than txn.readTs that implies that the
		// committedTxn finished before the current transaction started.
		// We don't need to check for conflict in that case.
		// This change assumes linearizability. Lack of linearizability could
		// cause the read ts of a new txn to be lower than the commit ts of
		// a txn before it (@mrjn).
		// 如果遍历到的事务结束 ts 小于等于 当前事务的 readTs 则跳过.
		if committedTxn.ts <= txn.readTs {
			continue
		}

		// 判断当前事务的 read keys 集合是否在于 committedTxn 中.
		for _, ro := range txn.reads {
			if _, has := committedTxn.conflictKeys[ro]; has {
				// 存在说明有事务冲突.
				return true
			}
		}
	}

	return false
}

func (o *oracle) newCommitTs(txn *Txn) (uint64, bool) {
	o.Lock()
	defer o.Unlock()

	// 检测传入的txn是否跟 oracle 记录的事务有冲突.
	if o.hasConflict(txn) {
		return 0, true
	}

	o.doneRead(txn)
	// 清理过期的事务
	o.cleanupCommittedTransactions()

	// This is the general case, when user doesn't specify the read and commit ts.
	ts := o.nextTxnTs
	o.nextTxnTs++
	o.txnMark.Begin(ts)

	utils.AssertTrue(ts >= o.lastCleanupTs)

	if o.detectConflicts {
		// We should ensure that txns are not added to o.committedTxns slice when
		// conflict detection is disabled otherwise this slice would keep growing.
		// 把当前事务放到 oracle 的 committedTxns 中.
		o.committedTxns = append(o.committedTxns, committedTxn{
			ts:           ts,
			conflictKeys: txn.conflictKeys,
		})
	}

	return ts, false
}

func (o *oracle) doneRead(txn *Txn) {
	if !txn.doneRead {
		txn.doneRead = true
		o.readMark.Done(txn.readTs)
	}
}

// cleanupCommittedTransactions 用来清理已提交事务集合里处于低水位线的事务集合.
// 避免在判断冲突事务时, 因遍历过多的 committedTxns 事务, 造成多余的性能开销.
func (o *oracle) cleanupCommittedTransactions() { // Must be called under o.Lock
	if !o.detectConflicts {
		// When detectConflicts is set to false, we do not store any
		// committedTxns and so there's nothing to clean up.
		return
	}
	// Same logic as discardAtOrBelow but unlocked
	var maxReadTs uint64

	// 水位线, readMark 是一个堆结构, 从堆里获取获取事务中最小事务的时间.
	maxReadTs = o.readMark.DoneUntil()

	utils.AssertTrue(maxReadTs >= o.lastCleanupTs)

	// do not run clean up if the maxReadTs (read timestamp of the
	// oldest transaction that is still in flight) has not increased
	if maxReadTs == o.lastCleanupTs {
		return
	}
	// 把当前记录的 ts 赋值到 lastCleanupTs
	o.lastCleanupTs = maxReadTs

	tmp := o.committedTxns[:0]
	for _, txn := range o.committedTxns {
		// 比水位线小, 需要被删除, 也就是忽略.
		if txn.ts <= maxReadTs {
			continue
		}
		// 比水位线高则保留
		tmp = append(tmp, txn)
	}
	o.committedTxns = tmp
}

func (o *oracle) doneCommit(cts uint64) {
	o.txnMark.Done(cts)
}

// Txn represents a  transaction.
type Txn struct {
	readTs   uint64
	commitTs uint64
	size     int64
	count    int64
	db       *DB

	reads []uint64 // contains fingerprints of keys read.
	// contains fingerprints of keys written. This is used for conflict detection.
	conflictKeys map[uint64]struct{}
	readsLock    sync.Mutex // guards the reads slice. See addReadKey.

	pendingWrites map[string]*utils.Entry // cache stores any writes done by txn.

	numIterators int32
	discarded    bool
	doneRead     bool
	update       bool // update is used to conditionally keep track of reads.
}

type pendingWritesIterator struct {
	entries  []*utils.Entry
	nextIdx  int
	readTs   uint64
	reversed bool
}

func (pi *pendingWritesIterator) Item() utils.Item {
	return pi.entries[pi.nextIdx]
}

func (pi *pendingWritesIterator) Next() {
	pi.nextIdx++
}

func (pi *pendingWritesIterator) Rewind() {
	pi.nextIdx = 0
}

func (pi *pendingWritesIterator) Seek(key []byte) {
	key = utils.ParseKey(key)
	pi.nextIdx = sort.Search(len(pi.entries), func(idx int) bool {
		cmp := bytes.Compare(pi.entries[idx].Key, key)
		if !pi.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (pi *pendingWritesIterator) Key() []byte {
	utils.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return utils.KeyWithTs(entry.Key, pi.readTs)
}

func (pi *pendingWritesIterator) Value() utils.ValueStruct {
	utils.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return utils.ValueStruct{
		Value:     entry.Value,
		Meta:      entry.Meta,
		ExpiresAt: entry.ExpiresAt,
		Version:   pi.readTs,
	}
}

func (pi *pendingWritesIterator) Valid() bool {
	return pi.nextIdx < len(pi.entries)
}

func (pi *pendingWritesIterator) Close() error {
	return nil
}

func (txn *Txn) newPendingWritesIterator(reversed bool) *pendingWritesIterator {
	if !txn.update || len(txn.pendingWrites) == 0 {
		return nil
	}
	entries := make([]*utils.Entry, 0, len(txn.pendingWrites))
	for _, e := range txn.pendingWrites {
		entries = append(entries, e)
	}
	// Number of pending writes per transaction shouldn't be too big in general.
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &pendingWritesIterator{
		readTs:   txn.readTs,
		entries:  entries,
		reversed: reversed,
	}
}

func (txn *Txn) checkSize(e *utils.Entry) error {
	count := txn.count + 1
	// Extra bytes for the version in key.
	size := txn.size + int64(e.EstimateSize(int(txn.db.valueThreshold())+10))
	if count >= txn.db.opt.MaxBatchCount || size >= txn.db.opt.MaxBatchSize {
		return utils.ErrTxnTooBig
	}
	txn.count, txn.size = count, size
	return nil
}

func exceedsSize(prefix string, max int64, key []byte) error {
	return errors.Errorf("%s with size %d exceeded %d limit. %s:\n%s",
		prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
}

const maxKeySize = 65000
const maxValSize = 1 << 20

func ValidEntry(db *DB, key, val []byte) error {
	switch {
	case len(key) == 0:
		return utils.ErrEmptyKey
	case len(key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow  move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, key)
	case int64(len(val)) > maxValSize:
		return exceedsSize("Value", maxValSize, val)
	}
	return nil
}

func (txn *Txn) modify(e *utils.Entry) error {
	switch {
	case !txn.update:
		return utils.ErrReadOnlyTxn
	case txn.discarded:
		return utils.ErrDiscardedTxn
	case len(e.Key) == 0:
		return utils.ErrEmptyKey
	case len(e.Key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow  move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, e.Key)
	}

	// 检查 key size
	if err := txn.checkSize(e); err != nil {
		return err
	}

	// The txn.conflictKeys is used for conflict detection. If conflict detection
	// is disabled, we don't need to store key hashes in this map.
	if txn.db.opt.DetectConflicts {
		//  通过 `runtime.memhash` 计算 key 的hash值, 然后写到 txn 的 conflictKeys 字典里.
		fp := utils.MemHash(e.Key) // Avoid dealing with byte arrays.
		txn.conflictKeys[fp] = struct{}{}
	}

	// 把写入的 entry 写到待写入字典里 pendingWrites 里.
	txn.pendingWrites[string(e.Key)] = e
	return nil
}

// Set adds a key-value pair to the database.
// It will return ErrReadOnlyTxn if update flag was set to false when creating the transaction.
//
// The current transaction keeps a reference to the key and val byte slice
// arguments. Users must not modify key and val until the end of the transaction.
func (txn *Txn) Set(key, val []byte) error {
	return txn.SetEntry(utils.NewEntry(key, val))
}

// SetEntry takes an utils.Entry struct and adds the key-value pair in the struct,
// along with other metadata to the database.
//
// The current transaction keeps a reference to the entry passed in argument.
// Users must not modify the entry until the end of the transaction.
func (txn *Txn) SetEntry(e *utils.Entry) error {
	return txn.modify(e)
}

// Delete deletes a key.
//
// This is done by adding a delete marker for the key at commit timestamp.  Any
// reads happening before this timestamp would be unaffected. Any reads after
// this commit would see the deletion.
//
// The current transaction keeps a reference to the key byte slice argument.
// Users must not modify the key until the end of the transaction.
func (txn *Txn) Delete(key []byte) error {
	e := &utils.Entry{
		Key:  key,
		Meta: utils.BitDelete,
	}
	return txn.modify(e)
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	} else if txn.discarded {
		return nil, utils.ErrDiscardedTxn
	}

	item = new(Item)
	item.e = new(utils.Entry)
	if txn.update {
		if e, has := txn.pendingWrites[string(key)]; has && bytes.Equal(key, e.Key) {
			if isDeletedOrExpired(e.Meta, e.ExpiresAt) {
				return nil, utils.ErrKeyNotFound
			}
			// Fulfill from cache.
			item.e.Meta = e.Meta
			item.e.Value = e.Value
			item.e.Key = key
			item.e.Version = txn.readTs
			item.e.ExpiresAt = e.ExpiresAt
			// We probably don't need to set db on item here.
			return item, nil
		}
		// Only track reads if this is update txn. No need to track read if txn serviced it
		// internally.
		txn.addReadKey(key)
	}

	seek := utils.KeyWithTs(key, txn.readTs)
	vs, err := txn.db.Get(seek)
	if err != nil {
		return nil, utils.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return nil, utils.ErrKeyNotFound
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return nil, utils.ErrKeyNotFound
	}

	item.e.Key = key
	item.e.Version = vs.Version
	item.e.Meta = vs.Meta
	item.e.Value = vs.Value
	item.e.ExpiresAt = vs.ExpiresAt
	return item, nil
}

func (txn *Txn) addReadKey(key []byte) {
	if txn.update {
		fp := utils.MemHash(key)

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		txn.readsLock.Lock()
		txn.reads = append(txn.reads, fp)
		txn.readsLock.Unlock()
	}
}

// Discard discards a created transaction. This method is very important and must be called. Commit
// method calls this internally, however, calling this multiple times doesn't cause any issues. So,
// this can safely be called via a defer right when transaction is created.
//
// NOTE: If any operations are run on a discarded transaction, ErrDiscardedTxn is returned.
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}
	if atomic.LoadInt32(&txn.numIterators) > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	txn.discarded = true

	txn.db.orc.doneRead(txn)

}

func (txn *Txn) commitAndSend() (func() error, error) {
	orc := txn.db.orc
	// Ensure that the order in which we get the commit timestamp is the same as
	// the order in which we push these updates to the write channel. So, we
	// acquire a writeChLock before getting a commit timestamp, and only release
	// it after pushing the entries to it.
	orc.writeChLock.Lock()
	defer orc.writeChLock.Unlock()

	// 获取 commitTS, 并检测当前事务是否跟全局活动事务表中事务产生冲突.
	commitTs, conflict := orc.newCommitTs(txn)
	if conflict {
		return nil, utils.ErrConflict
	}

	setVersion := func(e *utils.Entry) {
		if e.Version == 0 {
			e.Version = commitTs
		}
	}
	for _, e := range txn.pendingWrites {
		setVersion(e)
	}

	entries := make([]*utils.Entry, 0, len(txn.pendingWrites))

	processEntry := func(e *utils.Entry) {
		// Suffix the keys with commit ts, so the key versions are sorted in
		// descending order of commit timestamp.
		e.Key = utils.KeyWithTs(e.Key, e.Version)
		// Add bitTxn only if these entries are part of a transaction. We
		// support SetEntryAt(..) in managed mode which means a single
		// transaction can have entries with different timestamps. If entries
		// in a single transaction have different timestamps, we don't add the
		// transaction markers.

		entries = append(entries, e)
	}

	// The following debug information is what led to determining the cause of
	// bank txn violation bug, and it took a whole bunch of effort to narrow it
	// down to here. So, keep this around for at least a couple of months.
	// var b strings.Builder
	// fmt.Fprintf(&b, "Read: %d. Commit: %d. reads: %v. writes: %v. Keys: ",
	// 	txn.readTs, commitTs, txn.reads, txn.conflictKeys)
	for _, e := range txn.pendingWrites {
		processEntry(e)
	}

	req, err := txn.db.sendToWriteCh(entries)
	if err != nil {
		// 更新水位线
		orc.doneCommit(commitTs)
		return nil, err
	}
	ret := func() error {
		err := req.Wait()
		// Wait before marking commitTs as done.
		// We can't defer doneCommit above, because it is being called from a
		// callback here.
		orc.doneCommit(commitTs)
		return err
	}
	return ret, nil
}

func (txn *Txn) commitPrecheck() error {
	if txn.discarded {
		return errors.New("Trying to commit a discarded txn")
	}
	return nil
}

// Commit commits the transaction, following these steps:
//
// 1. If there are no writes, return immediately.
//
// 2. Check if read rows were updated since txn started. If so, return ErrConflict.
//
// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
//
// 4. Batch up all writes, write them to value log and LSM tree.
//
// 5. If callback is provided,  will return immediately after checking
// for conflicts. Writes to the database will happen in the background.  If
// there is a conflict, an error will be returned and the callback will not
// run. If there are no conflicts, the callback will be called in the
// background upon successful completion of writes or any error during write.
//
// If error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
// tree won't be updated, so there's no need for any rollback.
func (txn *Txn) Commit() error {
	// txn.conflictKeys can be zero if conflict detection is turned off. So we
	// should check txn.pendingWrites. 没有写入，当前事务无更新
	if len(txn.pendingWrites) == 0 {
		return nil // Nothing to do.
	}
	// Precheck before discarding txn. 检测是否被关闭
	if err := txn.commitPrecheck(); err != nil {
		return err
	}
	defer txn.Discard()

	txnCb, err := txn.commitAndSend() // 提交事务, 写入数据
	if err != nil {
		return err
	}
	// If batchSet failed, LSM would not have been updated. So, no need to rollback anything.

	// TODO: What if some of the txns successfully make it to value log, but others fail.
	// Nothing gets updated to LSM, until a restart happens.
	// 等待事务执行完毕, 无异常下移动水位线.
	return txnCb()
}

type txnCb struct {
	commit func() error
	user   func(error)
	err    error
}

func runTxnCallback(cb *txnCb) {
	switch {
	case cb == nil:
		panic("txn callback is nil")
	case cb.user == nil:
		panic("Must have caught a nil callback for txn.CommitWith")
	case cb.err != nil:
		cb.user(cb.err)
	case cb.commit != nil:
		err := cb.commit()
		cb.user(err)
	default:
		cb.user(nil)
	}
}

// CommitWith acts like Commit, but takes a callback, which gets run via a
// goroutine to avoid blocking this function. The callback is guaranteed to run,
// so it is safe to increment sync.WaitGroup before calling CommitWith, and
// decrementing it in the callback; to block until all callbacks are run.
func (txn *Txn) CommitWith(cb func(error)) {
	if cb == nil {
		panic("Nil callback provided to CommitWith")
	}

	if len(txn.pendingWrites) == 0 {
		// Do not run these callbacks from here, because the CommitWith and the
		// callback might be acquiring the same locks. Instead run the callback
		// from another goroutine.
		go runTxnCallback(&txnCb{user: cb, err: nil})
		return
	}

	// Precheck before discarding txn.
	if err := txn.commitPrecheck(); err != nil {
		cb(err)
		return
	}

	defer txn.Discard()

	commitCb, err := txn.commitAndSend()
	if err != nil {
		go runTxnCallback(&txnCb{user: cb, err: err})
		return
	}

	go runTxnCallback(&txnCb{user: cb, commit: commitCb})
}

// ReadTs returns the read timestamp of the transaction.
func (txn *Txn) ReadTs() uint64 {
	return txn.readTs
}

// NewTransaction creates a new transaction.  supports concurrent execution of transactions,
// providing serializable snapshot isolation, avoiding write skews.  achieves this by tracking
// the keys read and at Commit time, ensuring that these read keys weren't concurrently modified by
// another transaction.
//
// For read-only transactions, set update to false. In this mode, we don't track the rows read for
// any changes. Thus, any long running iterations done in this mode wouldn't pay this overhead.
//
// Running transactions concurrently is OK. However, a transaction itself isn't thread safe, and
// should only be run serially. It doesn't matter if a transaction is created by one goroutine and
// passed down to other, as long as the Txn APIs are called serially.
//
// When you create a new transaction, it is absolutely essential to call
// Discard(). This should be done irrespective of what the update param is set
// to. Commit API internally runs Discard, but running it twice wouldn't cause
// any issues.
//
//	txn := db.NewTransaction(false)
//	defer txn.Discard()
//	// Call various APIs.
func (db *DB) NewTransaction(update bool) *Txn {
	return db.newTransaction(update)
}

func (db *DB) newTransaction(update bool) *Txn {
	txn := &Txn{
		update: update,
		db:     db,
		count:  1, // One extra entry for BitFin.
	}
	if update {
		if db.opt.DetectConflicts {
			txn.conflictKeys = make(map[uint64]struct{})
		}
		txn.pendingWrites = make(map[string]*utils.Entry)
	}
	txn.readTs = db.orc.readTs()

	return txn
}

// View executes a function creating and managing a read-only transaction for the user. Error
// returned by the function is relayed by the View method.
// If View is used with managed transactions, it would assume a read timestamp of MaxUint64.
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return utils.ErrDBClosed
	}
	txn := db.NewTransaction(false)

	defer txn.Discard()

	return fn(txn)
}

// Update executes a function, creating and managing a read-write transaction
// for the user. Error returned by the function is relayed by the Update method.
// Update cannot be used with managed transactions.
func (db *DB) Update(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return utils.ErrDBClosed
	}
	txn := db.NewTransaction(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}
