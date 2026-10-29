/*
p2.go -- CS 417/545 Project 2.  Primary-Backup Key-Value Store.

This is the ONLY file you edit (plus yours_test.go for your own testing).
Add any helper types, fields, or package-level variables you need.
Do NOT change the signatures or receiver types of any function that
has an EXPORTED comment above it: those are the public API that our
tests call.
*/

package main

import (
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

/* ========================================================================
   Part 1.  Single-Server Key-Value Service
   ======================================================================== */

/*
KVServer is a single RPC server that stores key -> value.  Part 2
extends it with primary-backup replication by adding a backup
address.
*/
type KVServer struct {
	mu     sync.Mutex
	me     string /* this server's RPC address, e.g. "localhost:9001" */
	backup string /* backup's RPC address, "" if this server is the backup */
	dead   int32  /* set to 1 by Kill */

	store map[string]string    /* key -> value, guarded by mu */
	dedup map[Xid]cachedReply /* xid -> reply already sent, guarded by mu */
}

/*
cachedReply is what the dedup table remembers for one Xid.  Get uses
both fields; Put and ForwardOp only use Err.
*/
type cachedReply struct {
	Err   Err
	Value string
}

/*
lookupDup returns the cached reply for xid, if any.  Caller holds kv.mu.
*/
func (kv *KVServer) lookupDup(xid Xid) (cachedReply, bool) {
	r, ok := kv.dedup[xid]
	return r, ok
}

/*
EXPORTED
MakeKVServer constructs a server.  If backup is "", this server IS
the backup (or a standalone single-server) and will not forward
anything.  If backup is a non-empty address, this server IS the
primary and must forward every mutating op to that address.  The
caller is responsible for calling net/rpc.Register and starting the
listener.
*/
func MakeKVServer(me, backup string) *KVServer {
	kv := &KVServer{me: me, backup: backup}
	kv.store = make(map[string]string)
	kv.dedup = make(map[Xid]cachedReply)
	return kv
}

/*
EXPORTED
Get returns the current value for args.Key.  Missing keys return "".
Must be at-most-once: a retried call with the same Xid returns the
same reply as the first call.
*/
func (kv *KVServer) Get(args GetArgs, reply *GetReply) error {
	if kv.isDead() {
		reply.Err = ErrNetwork
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if r, ok := kv.lookupDup(args.Xid); ok {
		reply.Err = r.Err
		reply.Value = r.Value
		return nil
	}

	value, ok := kv.store[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = value
	}
	kv.dedup[args.Xid] = cachedReply{Err: reply.Err, Value: reply.Value}
	return nil
}

/*
EXPORTED
Put overwrites the value for args.Key.
Must be at-most-once (see Get).
On the primary: must also forward to the backup and wait for its
ack before replying.
*/
func (kv *KVServer) Put(args PutAppendArgs, reply *PutAppendReply) error {
	if kv.isDead() {
		reply.Err = ErrNetwork
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if r, ok := kv.lookupDup(args.Xid); ok {
		reply.Err = r.Err
		return nil
	}

	kv.store[args.Key] = args.Value
	reply.Err = OK
	kv.dedup[args.Xid] = cachedReply{Err: reply.Err}
	return nil
}

/* ========================================================================
   Part 2.  Primary-Backup Replication
   ======================================================================== */

/*
EXPORTED
ForwardOp is called by the primary on the backup for every mutating
client operation.  The backup applies the operation (respecting
at-most-once) and returns OK.
*/
func (kv *KVServer) ForwardOp(args ForwardArgs, reply *ForwardReply) error {
	/* ADD YOUR CODE HERE */
	panic("not implemented yet")
}

/* ========================================================================
   Clerk (client-side helper)
   ======================================================================== */

/*
Clerk is the client-side helper type.  It hides retries from the
application.  In P2 the clerk talks to a fixed primary address;
automatic re-routing on failover is scope for the next project.
*/
type Clerk struct {
	mu      sync.Mutex
	primary string /* primary server address */
	xid     uint64 /* atomic counter for fresh Xids */

	client *rpc.Client /* cached connection to primary; nil until first use, guarded by mu */
}

/* retryWait is how long the clerk waits between failed attempts. */
const retryWait = 20 * time.Millisecond

/*
call sends one RPC to the primary, dialing first if there is no cached
connection.  On a transport error it closes and drops the connection
so the next attempt redials.  Caller holds ck.mu.
*/
func (ck *Clerk) call(method string, args any, reply any) error {
	if ck.client == nil {
		c, err := rpc.Dial("tcp", ck.primary)
		if err != nil {
			return err
		}
		ck.client = c
	}
	if err := ck.client.Call(method, args, reply); err != nil {
		ck.client.Close()
		ck.client = nil
		return err
	}
	return nil
}

/*
EXPORTED
MakeClerk returns a new Clerk that will send operations to the given
primary address.
*/
func MakeClerk(primary string) *Clerk {
	ck := &Clerk{primary: primary}
	/*
	   Start the Xid counter at a random 64-bit value.  The wire types
	   carry no client id, so two clerks counting from 0 would reuse
	   the same Xids and the server would treat one clerk's new op as
	   the other's retry.
	*/
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	ck.xid = binary.LittleEndian.Uint64(b[:])
	return ck
}

/*
nextXid returns a fresh at-most-once id.  Safe for concurrent use.
Every operation MUST carry a fresh Xid, and a retried operation MUST
reuse it.
*/
func (ck *Clerk) nextXid() Xid {
	return Xid(atomic.AddUint64(&ck.xid, 1))
}

/*
EXPORTED
Get returns the current value for key, or "" if the key is missing.
Keeps retrying through RPC timeouts until it gets a reply.
*/
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := GetArgs{Key: key, Xid: ck.nextXid()}
	for {
		var reply GetReply /* fresh each attempt: gob skips zero fields */
		if err := ck.call("KVServer.Get", args, &reply); err == nil {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			}
			/* ErrNetwork or unknown Err: keep connection, retry */
		}
		time.Sleep(retryWait)
	}
}

/*
EXPORTED
Put overwrites the value for key.  Keeps retrying (see Get).
*/
func (ck *Clerk) Put(key, value string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := PutAppendArgs{Key: key, Value: value, Op: OpPut, Xid: ck.nextXid()}
	for {
		var reply PutAppendReply
		if err := ck.call("KVServer.Put", args, &reply); err == nil {
			if reply.Err == OK {
				return
			}
			/* ErrNetwork or unknown Err: keep connection, retry */
		}
		time.Sleep(retryWait)
	}
}

/* ========================================================================
   Lifecycle helpers.
   ======================================================================== */

/*
EXPORTED
Kill marks the KV server dead.  Running goroutines should notice via
kv.isDead() and exit; subsequent RPCs should return ErrNetwork.
*/
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) isDead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

/* ========================================================================
   Command-line driver.  Section 5 of the writeup documents the flags.
   ======================================================================== */

func main() {
	mode := flag.String("mode", "", "kvserver | clerk")
	addr := flag.String("addr", "", "kvserver: this server's RPC address, e.g. localhost:9001")
	forward := flag.String("forward", "", "kvserver: backup's RPC address to forward writes to; omit to run as the backup / standalone")
	primary := flag.String("primary", "", "clerk: primary server's RPC address")
	op := flag.String("op", "", "clerk: get | put")
	key := flag.String("key", "", "clerk: key")
	val := flag.String("val", "", "clerk: value for put")
	flag.Parse()

	switch *mode {
	case "kvserver":
		if *addr == "" {
			die("kvserver mode needs -addr")
		}
		kv := MakeKVServer(*addr, *forward)
		rpc.Register(kv)
		serveForever(*addr)
	case "clerk":
		if *primary == "" || *op == "" || *key == "" {
			die("clerk mode needs -primary -op -key")
		}
		ck := MakeClerk(*primary)
		switch *op {
		case "get":
			fmt.Println(ck.Get(*key))
		case "put":
			ck.Put(*key, *val)
		default:
			die("unknown -op: " + *op)
		}
	default:
		die("set -mode to kvserver or clerk")
	}
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

func serveForever(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		die("listen " + addr + ": " + err.Error())
	}
	/* main registered kv with the global rpc.Register, so use the global Accept. */
	rpc.Accept(lis)
}
