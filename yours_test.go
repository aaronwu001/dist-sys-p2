/*
Your test file.  Nothing here is graded.  Write your own tests as you
go.  Delete or change the examples below.

Useful commands:

    go test .                      run every test in this folder
    go test -run TestSmoke .       run only tests whose name has TestSmoke
    go test -v .                   print each test name as it runs
    go test -race .                catch data races (needed for Part 2)

A good habit: when you find a bug, add a test that would have caught
it.  Then you never chase the same bug twice.
*/

package main

import (
	"fmt"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"testing"
	"time"
)

/*
startServer starts a KVServer on addr with its own rpc.Server and
registers cleanup with t.  forward is the backup address ("" for a
backup / standalone server).
*/
func startServer(t *testing.T, addr, forward string) *KVServer {
	t.Helper()
	kv := MakeKVServer(addr, forward)
	srv := rpc.NewServer()
	if err := srv.Register(kv); err != nil {
		t.Fatalf("register: %v", err)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Accept(lis)
	t.Cleanup(func() {
		kv.Kill()
		lis.Close()
	})
	return kv
}

/*
Step 1: call the server's RPCs directly (no Clerk).  Put then Get
returns the value; a missing key returns ErrNoKey and "".
*/
func TestServerDirectPutGet(t *testing.T) {
	addr := "127.0.0.1:21101"
	startServer(t, addr, "")

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	var putReply PutAppendReply
	putArgs := PutAppendArgs{Key: "a", Value: "1", Op: OpPut, Xid: 1}
	if err := client.Call("KVServer.Put", putArgs, &putReply); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if putReply.Err != OK {
		t.Fatalf("Put Err = %v, want OK", putReply.Err)
	}

	var getReply GetReply
	if err := client.Call("KVServer.Get", GetArgs{Key: "a", Xid: 2}, &getReply); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if getReply.Err != OK || getReply.Value != "1" {
		t.Fatalf("Get(a) = (%v, %q), want (OK, \"1\")", getReply.Err, getReply.Value)
	}

	var missReply GetReply
	if err := client.Call("KVServer.Get", GetArgs{Key: "nope", Xid: 3}, &missReply); err != nil {
		t.Fatalf("Get missing: %v", err)
	}
	if missReply.Err != ErrNoKey || missReply.Value != "" {
		t.Fatalf("Get(nope) = (%v, %q), want (ErrNoKey, \"\")", missReply.Err, missReply.Value)
	}
}

/*
A late retry of an old Put must not roll the value back, and a retried
Get must return the reply from the first time.
*/
func TestServerDedup(t *testing.T) {
	addr := "127.0.0.1:21102"
	startServer(t, addr, "")

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	put := func(key, value string, xid Xid) {
		t.Helper()
		var r PutAppendReply
		args := PutAppendArgs{Key: key, Value: value, Op: OpPut, Xid: xid}
		if err := client.Call("KVServer.Put", args, &r); err != nil || r.Err != OK {
			t.Fatalf("Put(%s,%s,xid=%d) = (%v, %v)", key, value, xid, err, r.Err)
		}
	}
	get := func(key string, xid Xid) string {
		t.Helper()
		var r GetReply
		if err := client.Call("KVServer.Get", GetArgs{Key: key, Xid: xid}, &r); err != nil {
			t.Fatalf("Get(%s,xid=%d): %v", key, xid, err)
		}
		return r.Value
	}

	/* Late retry of xid 5 arrives after xid 6. */
	put("x", "1", 5)
	put("x", "2", 6)
	put("x", "1", 5)
	if got := get("x", 7); got != "2" {
		t.Fatalf("after late retry, x = %q, want \"2\"", got)
	}

	/* Retried Get returns the first reply, even after x changes. */
	put("x", "3", 8)
	if got := get("x", 7); got != "2" {
		t.Fatalf("retried Get(xid=7) = %q, want cached \"2\"", got)
	}
	if got := get("x", 9); got != "3" {
		t.Fatalf("fresh Get = %q, want \"3\"", got)
	}
}

/*
After Kill, every RPC returns ErrNetwork -- even a retry of an Xid that
is already in the dedup table.
*/
func TestServerKilledReturnsErrNetwork(t *testing.T) {
	addr := "127.0.0.1:21103"
	kv := startServer(t, addr, "")

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	var r0 PutAppendReply
	args := PutAppendArgs{Key: "a", Value: "1", Op: OpPut, Xid: 1}
	if err := client.Call("KVServer.Put", args, &r0); err != nil || r0.Err != OK {
		t.Fatalf("Put before Kill = (%v, %v), want OK", err, r0.Err)
	}

	kv.Kill()

	var r1 PutAppendReply
	if err := client.Call("KVServer.Put", args, &r1); err != nil || r1.Err != ErrNetwork {
		t.Fatalf("retried Put after Kill = (%v, %v), want ErrNetwork", err, r1.Err)
	}
	var r2 GetReply
	if err := client.Call("KVServer.Get", GetArgs{Key: "a", Xid: 2}, &r2); err != nil || r2.Err != ErrNetwork {
		t.Fatalf("Get after Kill = (%v, %v), want ErrNetwork", err, r2.Err)
	}
}

/*
Clerk.Get on a missing key returns "".
*/
func TestClerkMissingKey(t *testing.T) {
	addr := "127.0.0.1:21201"
	startServer(t, addr, "")

	ck := MakeClerk(addr)
	if got := ck.Get("nope"); got != "" {
		t.Fatalf("Get(nope) = %q, want \"\"", got)
	}
}

/*
The clerk keeps retrying while the server is not up yet (Dial fails),
and finishes once the server starts.
*/
func TestClerkRetriesUntilServerUp(t *testing.T) {
	addr := "127.0.0.1:21202"
	ck := MakeClerk(addr)

	done := make(chan struct{})
	go func() {
		ck.Put("k", "v")
		close(done)
	}()

	time.Sleep(200 * time.Millisecond) /* clerk is failing to Dial now */
	select {
	case <-done:
		t.Fatalf("Put returned before the server existed")
	default:
	}

	startServer(t, addr, "")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Put did not finish after the server started")
	}
	if got := ck.Get("k"); got != "v" {
		t.Fatalf("Get(k) = %q, want \"v\"", got)
	}
}

/*
Two clerks interleave Puts.  With Xid counters that both start at 0,
clerk B's ops would hit clerk A's dedup entries and be dropped.
*/
func TestTwoClerksNoXidClash(t *testing.T) {
	addr := "127.0.0.1:21203"
	startServer(t, addr, "")

	a := MakeClerk(addr)
	b := MakeClerk(addr)

	a.Put("x", "a1")
	b.Put("x", "b1")
	if got := a.Get("x"); got != "b1" {
		t.Fatalf("after b.Put, a.Get(x) = %q, want \"b1\"", got)
	}
	b.Put("y", "b2")
	a.Put("y", "a2")
	if got := b.Get("y"); got != "a2" {
		t.Fatalf("after a.Put, b.Get(y) = %q, want \"a2\"", got)
	}
}

/*
Many clerks, plus several goroutines sharing one clerk, all hit one
server at once.  Each goroutine owns a key and checks that every Get
sees its own latest Put.  Run with -race.
*/
func TestConcurrentClerks(t *testing.T) {
	addr := "127.0.0.1:21204"
	startServer(t, addr, "")

	const nClerks = 8 /* goroutines with their own clerk */
	const nShared = 4 /* goroutines sharing one clerk */
	const nOps = 100

	shared := MakeClerk(addr)
	errs := make(chan string, nClerks+nShared)
	var wg sync.WaitGroup

	worker := func(ck *Clerk, key string) {
		defer wg.Done()
		for i := 0; i < nOps; i++ {
			want := strconv.Itoa(i)
			ck.Put(key, want)
			if got := ck.Get(key); got != want {
				errs <- fmt.Sprintf("%s: Get = %q, want %q", key, got, want)
				return
			}
		}
	}

	for i := 0; i < nClerks; i++ {
		wg.Add(1)
		go worker(MakeClerk(addr), "own"+strconv.Itoa(i))
	}
	for i := 0; i < nShared; i++ {
		wg.Add(1)
		go worker(shared, "shared"+strconv.Itoa(i))
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

/*
A smoke test.  Starts a single KVServer (no backup), writes a key,
reads it back, and checks the value.  Passes once Part 1 is done.
*/
func TestSmokeSingleServer(t *testing.T) {
	addr := "127.0.0.1:21001"

	kv := MakeKVServer(addr, "")
	defer kv.Kill()

	srv := rpc.NewServer()
	if err := srv.Register(kv); err != nil {
		t.Fatalf("register: %v", err)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	go srv.Accept(lis)

	time.Sleep(100 * time.Millisecond)

	ck := MakeClerk(addr)
	ck.Put("hello", "world")
	if got := ck.Get("hello"); got != "world" {
		t.Fatalf("Get(hello) = %q, want %q", got, "world")
	}
}

/*
Template for Part 2.  Uncomment and fill in once ForwardOp works.

func TestSmokePrimaryBackup(t *testing.T) {
    backupAddr  := "127.0.0.1:21011"
    primaryAddr := "127.0.0.1:21012"

    backup := MakeKVServer(backupAddr, "")
    ... register, listen ...

    primary := MakeKVServer(primaryAddr, backupAddr)
    ... register, listen ...

    ck := MakeClerk(primaryAddr)
    ck.Put("k", "v")
    // Reach into the backup directly and check its state matches.
}
*/
