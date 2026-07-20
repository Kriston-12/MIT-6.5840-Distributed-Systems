package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	holderID string
	// version rpc.Tversion
	lockname string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	holderID := kvtest.RandValue(12)
	lk := &Lock{ck: ck, holderID: holderID, lockname: lockname}
	return lk
}

func (lk *Lock) Acquire() {
	lock_name := lk.lockname
	GET:
		val, version, rpcErr := lk.ck.Get(lock_name)
	// rpcErr has only ok and ErrNoKey
	if rpcErr != rpc.OK {
		// If ErrNoKey, the lock does not exist, can directly create the lock
		putRpcErr := lk.ck.Put(lock_name, lk.holderID, 0) // fist time set version to 0
		if putRpcErr != rpc.OK {
			if (putRpcErr == rpc.ErrVersion) {
				// If ErrVersion, the lock has been created by another client, need to re-acquire the lock
				goto GET
			} 
			// ErrMaybe, call lk.ck.Get(lock_name) and compare the value with lk.holderID,
			// if equal, then the lock is acquired, otherwise, need to re-acquire the lock
			goto GET
		}
		// OK, successfully created the lock, lock acquired successfully
		return 
	}
	// If OK, the lock already exists, need to check if the lock holder is itself
	if val != lk.holderID {
		if (val == "") {
			putRpcErr := lk.ck.Put(lock_name, lk.holderID, version)
			if (putRpcErr != rpc.OK) {
				goto GET
			}
			return 
		} else {
			time.Sleep(10 * time.Millisecond)
			goto GET
		}
	}
}

func (lk *Lock) Release() {
	lock_name := lk.lockname
	// Get will always return OK, because the lock must exist when releasing
	_, version, _ := lk.ck.Get(lock_name)

	// Put可能返回ErrMaybe and ok. Both cases indicate that the lock has been released successfully.
	// Cannot return ErrVersion, ErrVersion can only occur if another client preempts the lock with Put(lock_name, _, version) before Get and Put, but in Release, lk.holderID must be the holder of lock_name, so ErrVersion will not occur
	lk.ck.Put(lock_name, "", version)
}
