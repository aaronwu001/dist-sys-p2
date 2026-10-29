# P2 設計決定

Primary-Backup Key-Value Store 的設計紀錄。實作與除錯時以此為準；改設計時同步更新這份檔案。

---

## 1. Clerk

### Xid
- `MakeClerk` 時用 `crypto/rand` 產生 64 位元隨機數，作為 `ck.xid` 計數器起點。
- 每個新操作呼叫 `nextXid()` 取得新 Xid（+1）。
- Xid 在重試迴圈**之前**產生一次，所有重試沿用同一個 Xid。

**理由**：`GetArgs` / `PutAppendArgs` 沒有 client ID 欄位（sealed.go 不可改）。若每個 clerk 都從 0 開始，兩個 clerk 的 Xid 會撞號，server 會把 B 的新操作誤判為 A 的重試。隨機 64 位元起點讓不同 clerk 的 Xid 區段實際上不會重疊。

### 鎖
- `Get` / `Put` 一進函式就拿 `ck.mu`，`defer` 放鎖。
- 同一個 clerk 的操作因此序列化；需要併發時使用多個 clerk（測試也是這樣做）。

### 連線
- 存一個 `*rpc.Client`，初始為 `nil`，要用時才 `Dial`（lazy dial）。
- 不每次呼叫都 Dial —— 這是 perf 分數的關鍵。

### Timeout
- **不設 timeout**，直接用 `client.Call`。
- **理由**：P2 範圍內 net/rpc 跑在 TCP 上，回覆只會因連線斷掉而失敗（此時 `Call` 會回 error）；server 被 Kill 時仍會回 ErrNetwork；primary 等 backup 時 clerk 本來就該等。

### 回覆處理

| 收到 | 動作 |
|---|---|
| Go error（`Call` 回非 nil） | `Close`、設 `nil`、稍等、重試 |
| `ErrNetwork` | 保留連線、稍等、重試 |
| `ErrNoKey` | 回 `""` |
| `OK` | 回結果 |
| 其他未知 Err | 稍等、重試（防禦性 default） |

- 「稍等」設短：10–50ms。正常路徑不會走到，不影響 perf，只影響測試時間。

---

## 2. Server（primary / backup / 單機共用同一份程式）

- `backup != ""` → 這台是 primary，Put 要轉發。
- `backup == ""` → 這台是 backup 或單機，不轉發。

### 狀態
```go
type cachedReply struct {
    Err   Err
    Value string // 只有 Get 用到
}

// KVServer 新增欄位
store         map[string]string
dedup         map[Xid]cachedReply // 一張表，Get / Put / ForwardOp 共用
backupClient  *rpc.Client         // 只有 primary 用；初始 nil，lazy dial；受 kv.mu 保護
```

### `MakeKVServer`
- 建立 `&KVServer{me: me, backup: backup}`，初始化 `store`、`dedup` 兩個 map。
- `backupClient` 保持 `nil`，**不在建構時 Dial**（測試裡 backup 不一定先起好；第一次 forward 時才連）。
- 不註冊 RPC、不 listen —— 那是呼叫端的責任（見 p2.go 註解）。

### 去重表：一張表
- Get、Put、ForwardOp 的回覆都存在同一張 `map[Xid]cachedReply`。
- `PutAppendReply`、`ForwardReply` 只有 `Err`；`GetReply` 多一個 `Value`，一個結構全裝得下。
- **理由**：查表邏輯只寫一次，ForwardOp 直接共用；Xid 來自同一個計數器，Get / Put 不會撞號；效能與記憶體和分表沒有實質差別。
- 表不做 GC。perf 約 2 萬筆、545 的 100k 步約 20 萬筆，記憶體可承受。

### 不存在的 key
- 回 `ErrNoKey` + `Value = ""`（Clerk 收到 ErrNoKey 回 `""`）。

### Handler 順序（Get / Put / ForwardOp 共用骨架）
1. `isDead()` → 回 `ErrNetwork`
2. 拿 `kv.mu`
3. 查去重表，有就回快取的回覆
4. 執行（讀 / 寫 `store`）
5. **只有 primary 的 Put**：用同一個 Xid 重試 `ForwardOp` 直到 OK
   - 每輪檢查 `isDead()`，死了就停止並回 `ErrNetwork`
   - 失敗之間稍等
6. 記錄到去重表（primary 的 Put：在 backup ack **之後**才記錄）
7. 放鎖、回覆

### 所有 handler 一律 `return nil`
- 錯誤一律放在 `reply.Err`。
- **理由**：handler 回非 nil error 時 client 拿到 `rpc.ServerError`，會落到「Go error → Close 連線」那條路，造成不必要的重連。

### 請求欄位怎麼填
- Clerk 的 Put：`PutAppendArgs{Key, Value, Op: OpPut, Xid}`。
- primary 轉發：`ForwardArgs` 四個欄位直接從 client 的 `PutAppendArgs` 照抄（`Key, Value, Op, Xid`），**Xid 不換新的**，backup 才能用原始 Xid 去重。

### 為什麼 forward 期間持有 `kv.mu`
- 保證 backup 套用操作的順序和 primary 完全一致。若 apply 後先放鎖再 forward，兩個併發 Put 可能以相反順序到達 backup，兩邊狀態分岔。
- 也讓「同一 Xid 的重試在第一次還在 forward 時抵達」這種情況自然被擋住（重試會等鎖，拿到時表裡已有結果）。

### 本地 apply 與 forward 的順序：先 apply 再 forward
- 依照 desc Part 2 列出的順序：驗證 → apply 本地 → ForwardOp → 等 ack → 回覆。
- desc 明訂 P2 不測 crash recovery、view change 不在範圍（「Both servers start together… view changes are NOT in scope」）。唯一要遵守的不變量是 **等 backup ack 之後才回覆 client**（Part 2 Note 1）。
- 因此不處理「primary 在 forward 途中被 Kill」造成的 primary / backup 不一致。`Kill` 在測試裡只用於收尾，要求只是之後的 RPC 回 ErrNetwork。

---

## 3. Primary → Backup 連線
- primary 存一個 cached `*rpc.Client` 連 backup。
- 出錯就 `Close`、設 `nil`，下一輪重 `Dial`。
- 所有存取都已在 `kv.mu` 內，不需額外的鎖。

## 4. Backup 的 ForwardOp
- 與 Put 相同流程（第 2 節的 handler 順序），但跳過第 5 步，不再往下 forward。
- 用 primary 傳來的 Xid（即原始 client 的 Xid）去重。

## 5. `serveForever`（命令列模式用）
- `net.Listen("tcp", addr)`，失敗就 `die`。
- 用**全域**的 `rpc.Accept(lis)`（它本身就是 accept 迴圈，會阻塞）。
- **理由**：`main` 呼叫的是全域 `rpc.Register(kv)`，服務註冊在 `rpc.DefaultServer`；若這裡改用 `rpc.NewServer()` 會找不到 KVServer。
- 測試不走這條路（測試自己 `rpc.NewServer` + `srv.Accept`），所以這個函式只影響 desc 第 5 節的手動執行。

---

## 6. p2.go `ADD YOUR CODE` 對照表

| p2.go 位置 | 設計見 |
|---|---|
| `KVServer` struct 欄位 | §2 狀態 |
| `MakeKVServer` | §2 `MakeKVServer` |
| `KVServer.Get` | §2 Handler 順序 |
| `KVServer.Put` | §2 Handler 順序、請求欄位、§3 |
| `KVServer.ForwardOp` | §4 |
| `Clerk` struct 欄位 | §1 連線 |
| `MakeClerk` | §1 Xid |
| `Clerk.Get` | §1 回覆處理 |
| `Clerk.Put` | §1 回覆處理、§2 請求欄位 |
| `serveForever` | §5 |

需要的新 import：`crypto/rand`（Xid 起點；可配 `encoding/binary` 轉 uint64）、`net`、`time`（稍等）。

---

## 7. 實作與測試順序

| 步驟 | 動到的 `ADD YOUR CODE` | 內容 | 驗證 |
|---|---|---|---|
| 0 | — | 確認 P1 的 at-most-once 正確；`go build .` | 編譯過 |
| 1 | `KVServer` 欄位（`store`、`dedup`）、`MakeKVServer`、`KVServer.Get`、`KVServer.Put` | server 單機讀寫 + 去重表（不轉發） | `TestServerDirectPutGet`、`TestServerDedup`（舊 Xid 晚到不倒退、重送 Get 回快取） |
| 2 | `Clerk` 欄位（`*rpc.Client`）、`Clerk.Get`、`Clerk.Put` | lazy dial、重試迴圈、回覆處理表、`Op: OpPut` | `TestSmokeSingleServer` 通過 |
| 3 | `serveForever` | net.Listen + 全域 `rpc.Accept` | 手動：`go run . -mode kvserver -addr localhost:9002`，另一個終端 `-mode clerk -primary localhost:9002 -op put/get` |
| 4 | `MakeClerk`（隨機 Xid 起點） | 隨機 Xid（server 去重已在步驟 1 完成） | 兩個 clerk 交錯 Put，彼此不被誤判為重送 |
| 5 | `KVServer.Get`/`Put`（isDead 檢查） | 併發安全、Kill 後回 ErrNetwork | 多 goroutine × 多 clerk，`go test -race`；Kill 後直接 RPC 得 ErrNetwork |
| ✅ | | **Part 1 完成 → commit** | |
| 6 | `KVServer.ForwardOp` | backup 端套用 + 去重 | 直接呼叫 ForwardOp 兩次，只套用一次 |
| 7 | `KVServer` 欄位（`backupClient`）、`KVServer.Put`（轉發） | 持鎖、同 Xid 重試 ForwardOp、等 ack 才記錄 | `TestSmokePrimaryBackup`：Put 後直接對 backup Get 比對；手動跑 desc §5 的四行指令（含 `-forward`） |
| 8 | — | 壓力測試 | 多 clerk 併發 burst 寫入，逐 key 比對 primary 與 backup；`go test -race -count=20` |
| ✅ | | **Part 2 完成 → commit** | |
| 9 | — | Perf | 本機 `go test -run Perf .` 確認收斂，再到 iLab 跑並記數字 |
| 10 | — | Report.md：姓名、NetID、perf 數字、AI 使用說明 | |

### 要在 `yours_test.go` 補的測試
- `startServer(t, addr, forward)` helper（參考 perf_test 的 `startKV`），每個測試用不同 port。
- `TestDuplicatePut`：同 Xid 直接送兩次 RPC。
- `TestTwoClerksNoXidClash`：兩個 clerk 交錯 Put。
- `TestBackupMatchesPrimary`：burst 寫入後逐 key 比對。
- `TestKilledServerReturnsErrNetwork`。
