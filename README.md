# shrt-java

High-performance URL shortener backend — Java port of
[shrt-ts](https://github.com/abhijitkrm/shrt-ts) /
[shrt-go](https://github.com/abhijitkrm/shrt-go) /
[shrt-rust](https://github.com/abhijitkrm/shrt-rust) /
[shrt-cpp](https://github.com/abhijitkrm/shrt-cpp). **Zero dependencies** —
JDK 20 stdlib only.

* **HTTP**: custom thread-per-connection server (`SERVER=mini`, default) —
  one read batch answered by a single write, keep-alive + HTTP pipelining;
  `SERVER=jdk` uses the built-in `com.sun.net.httpserver.HttpServer`.
* **Storage**: custom append-only log (AOF) — `ConcurrentHashMap` index
  (Java's striped map — the native answer to the 256-shard scheme in the
  sibling ports) + `AtomicLong` hit counters + batched `FileChannel`
  write/`force`. Reads never touch disk; writes are ~ns enqueue + one
  syscall batch per 5 ms.
* **Serialization**: log lines are hand-serialized into `StringBuilder`
  scratch — zero JSON, zero per-row allocation on the read/bulk hot paths.
  Requests use a minimal purpose-built recursive-descent JSON parser.
* **Codes**: 8 chars = `ALPHABET[instance]` + 7 random base62 chars
  (62⁷ ≈ 3.5T per instance). The prefix shard-marks every code — unique
  across processes with zero coordination, and a read-miss knows exactly
  which sibling log to tail. Checked against the index, retried on collision.
* **Multi-instance**: `WORKERS=N` spawns N JVM processes on **consecutive
  ports** (`PORT+i` — the JDK doesn't expose `SO_REUSEPORT`, same
  port-per-worker scheme as the original TS) with per-instance log shards
  (`data-<i>.log`). Siblings are discovered and tailed **lazily on
  read-miss** — writes never pay replication cost.
* **Durability**: every append reaches the OS page cache within 5 ms and is
  `force()`d every 500 ms — process crash loses ≤5 ms of writes, machine
  crash ≤~500 ms (tunables in `Store.java`; snapshot+truncate via
  `compact()`). Instance claiming uses `FileLock` on `instance-<i>.lock`.

## Build & run

```sh
make                    # javac -> classes/
java -cp classes shrt.Main          # :3000, mini server, 1 instance
WORKERS=4 java -cp classes shrt.Main   # 4 instances on :3000-:3003
SERVER=jdk java -cp classes shrt.Main  # JDK HttpServer frontend
```

## API (identical to sibling implementations)

| route | method | description |
|---|---|---|
| `/api/shorten` | POST | `{"url","alias"?,"ttl_ms"?}` → `{"code","short_url"}` |
| `/api/shorten/bulk` | POST | `{"urls":[...]}` (≤10k) → `{"count","codes"}` |
| `/:code` | GET | 302 redirect (counts a hit) |
| `/api/stats/:code` | GET | `{"code","url","hits","created_at","expires_at"}` |
| `/api/links` | GET | `?limit&offset&sort=hits|created&q` (admin list) |
| `/api/links/:code` | PATCH/DELETE | requires `ADMIN_TOKEN` + `x-admin-token` header |
| `/api/health` `/api/metrics` | GET | health / request counters |
| `/` | GET | built-in UI (`ui/index.html`) |

## Config (env)

`PORT` (3000) · `DATA_DIR` (`data`) · `WORKERS` (1) · `SERVER` (`mini`|`jdk`)
· `INSTANCE` (auto) · `SEED` (pre-generate N links at boot) · `HITS` (`1`)
· `TAIL_MS` (0) · `ADMIN_TOKEN` · `CORS_ORIGIN` (`*`)
· `LINK_TTL_MS` (86400000, default AND cap)
· `STORE` (`aof`|`dragonfly`|`redis`|`rocksdb`) · `DRAGONFLY_ADDR` (`127.0.0.1:6379`)
· `ROCKSDB_PATH` (`{DATA_DIR}/rocks`) — embedded RocksDB dir; needs
  `lib/rocksdbjni-*.jar` (Makefile fetches it from Maven automatically)
· `CACHE` (100000, bounded hot FIFO entries) · `CACHE_TTL_MS` (5000)
`KV_LAYOUT` (`key`|`hash`) packs links as hash fields in `l:{code % KV_BUCKETS}` (~40% less KV memory); expiry via read-check + `KV_SWEEP_MS` janitor (1h). Server needs `hash-max-listpack-value` >= ~256 for full savings.


`STORE=dragonfly` moves the whole corpus to an external RESP store
(DragonflyDB / Redis): keys `l:{code}` → `{exp}|{created}|{url}` (PX
self-evicts TTLs), `h:{code}` → hit counter (batched `INCRBY` every 5 ms).
Each node keeps only a bounded FIFO cache — memory stays flat as links
grow; a cold redirect costs one `GET`. No tailing — admin mutations work on
any node. Live tests: `SHRT_KV_ADDR=127.0.0.1:6379 java -cp classes shrt.TestMain`.

`STORE=rocksdb` keeps the corpus on local disk in an embedded RocksDB via
rocksdbjni (CF `links`: `code` → `{exp}|{created}|{url}`; CF `hits`: u64
counters via `UInt64AddOperator` merges — no read-modify-write). Expiry is
enforced on read plus a `ROCKSDB_SWEEP_MS` iterator sweep (rocksdbjni has
no compaction-filter API). Reads hit the same bounded FIFO cache first;
bulk shorten is one `WriteBatch`. RocksDB holds an exclusive LOCK on the
dir — one process per `ROCKSDB_PATH`; for multi-instance/multi-node use
the RESP backend.

## Bench

```sh
make bench          # or: java -XX:+UseParallelGC -cp classes shrt.Bench
bash scripts/smoke.sh   # end-to-end API smoke
bash scripts/flood.sh   # write flood (autocannon if installed)
make test               # 47 tests: store engine + API × both frontends
```

Measured on Apple Silicon (client+server colocated, 64 conns, same in-repo
loadgen methodology across implementations):

| scenario | shrt-java | shrt-cpp | shrt-rust | shrt-go | shrt-ts |
|---|---|---|---|---|---|
| redirect | ~160k | ~158k | ~208k | ~197k | ~133k |
| redirect, pipelined ×10 | ~440k | ~607k | ~409k | ~457k | ~320k |
| mixed 95/5 | ~149k | ~157k | ~205k | ~185k | ~115k |
| shorten | ~135k | ~165k | ~175k | ~158k | ~72k |
| bulk ×1000 | ~1.2M | ~3.4M | ~4.1M | ~2.1M | ~640k |
| redirect ×4 workers | ~180k\* | ~214k | ~184k | — | — |

\* Java spreads workers across 4 ports (no `SO_REUSEPORT` in the JDK) — the
loadgen round-robins connections across all of them.

Java lands between the TS original and the native ports: competitive with
Go/C++ on single-shot reads, ahead of TS everywhere; bulk writes are the
GC-bound case where native still wins ~3×.

## Layout

```
src/shrt/
  Codec.java      AOF row/hit/del line encode+parse (zero-dep)
  Aof.java        append-only log: buffered push, replay, tail readers,
                  FileLock instance claiming
  Store.java      CHM index, hits batching, sibling tailing, compact
  App.java        transport-agnostic handler + minimal JSON parser
  ServerMini.java thread-per-conn server (batched pipelined writes)
  ServerJdk.java  com.sun.net.httpserver frontend
  Main.java       env config, WORKERS supervisor (PORT+i), seed, shutdown
  Bench.java      load generator + benchmark scenarios
tests/
  StoreTests.java storage-engine tests
  ApiTests.java   API tests run against mini AND jdk frontends
ui/index.html     admin UI (same as sibling repos)
```
