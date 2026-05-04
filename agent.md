# Agent Guide — postgres-async-driver

## 1. Project Overview

**postgres-async-driver** is a high-performance, non-blocking Java driver for PostgreSQL that operates directly over the PostgreSQL wire protocol (v3). It returns results as `CompletableFuture`s, enabling fully asynchronous, callback-driven database interactions without blocking threads.

### Key Characteristics

- **Async-first**: All operations return `CompletableFuture<T>`, never block
- **Framework-agnostic**: Network I/O is pluggable via `ProtocolStream` interface (Netty, Grizzly, etc.)
- **Zero blocking dependencies**: Core library only depends on JAXB (for base64 encoding); Netty is test-only
- **Connection pooling**: Built-in pool with configurable sizing and statement caching
- **Full protocol support**: Handles simple queries, extended queries, prepared statements, transactions, LISTEN/NOTIFY
- **Type-safe**: Comprehensive type converters for all PostgreSQL types (numeric, temporal, arrays, custom)

### When to Use

- Microservices requiring high concurrency without thread-per-request overhead
- Event-driven systems where database ops must integrate with async/reactive pipelines
- Applications prioritizing GC pause reduction through non-blocking I/O
- Projects already using Netty, Vert.x, or similar reactive frameworks

### When NOT to Use

- Simple CRUD apps where JDBC simplicity is preferred over async complexity
- Synchronous business logic that would just wrap futures in `join()` calls
- When connection pooling overhead outweighs multi-query throughput gains

### Build & Toolchain

```powershell
# Build (compile + test)
./gradlew build

# Compile only
./gradlew classes

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "com.github.pgasync.QueryResultTest"
```

**Version**: 1.0.5 | **License**: Apache 2.0 | **JDK**: 11+ | **Group/Artifact**: `com.github.pgasync:postgres-async-driver`

---

## 2. Architecture Overview

### High-Level Component Stack

```
┌─────────────────────────────────────────────┐
│         Public API (com.pgasync)            │
│  Connectible, Connection, Transaction       │
│  QueryExecutor, ResultSet, Row              │
└──────────────┬──────────────────────────────┘
               │ (implements)
┌──────────────▼──────────────────────────────┐
│    Core Implementation (com.github.pgasync) │
│  • PgConnectible (abstract base)            │
│  • PgDatabase (single connection)           │
│  • PgConnectionPool (bounded pool)          │
│  • PgConnection (active query executor)     │
│  • PgProtocolStream (message sequencing)    │
└──────────────┬──────────────────────────────┘
               │ (uses)
┌──────────────▼──────────────────────────────┐
│         Protocol Layer                      │
│  • ProtocolStream (NIO framework adapter)   │
│  • Message (immutable wire DTOs)            │
│    - Frontend: Query, Bind, Describe, etc.  │
│    - Backend: RowDescription, DataRow, etc. │
│  • Encoder/Decoder (wire format codecs)     │
│  • IO utilities (byte-level manipulation)   │
└──────────────┬──────────────────────────────┘
               │ (encodes/decodes)
┌──────────────▼──────────────────────────────┐
│    Type Conversion Layer                    │
│  • DataConverter (OID ↔ Java type mapping)  │
│  • per-type converters (numeric, temporal,  │
│    string, array, blob, boolean)            │
│  • Oid constants + SASL prep utilities      │
└──────────────┬──────────────────────────────┘
               │ (serializes/deserializes)
┌──────────────▼──────────────────────────────┐
│   Network I/O (pluggable)                   │
│   Implemented by: NettyPgProtocolStream     │
│   (other frameworks via ProtocolStream impl)│
└─────────────────────────────────────────────┘
```

### Data Flow: Query → Response

```
User code (async)
    │
    ▼
completeQuery("SELECT ...", params)  [Public API]
    │
    ▼
PgConnectible.query()  [routes to pool or plain]
    │
    ├─ getConnection()  [reuse pool connection or create new]
    │
    ▼
PgConnection.query(sql, params)
    │
    ├─ prepareStatement(sql, ...)  [generates statement name]
    │
    ▼
stream.send(Parse)  [ProtocolStream: encode as bytes, push to network]
    │
    ▼
ProtocolStream.gotMessage(ParseComplete)  [network: server responds]
    │
    ▼
PgConnection.fetch(parameters)  [binds params, describes, executes]
    │
    ▼
stream.send(Bind, Describe, Execute, SYNC)  [extended query flow]
    │
    ▼
Network → RowDescription, DataRow*, CommandComplete, ReadyForQuery
    │
    ├─ Decoder chain converts wire bytes to Message objects
    │
    ▼
PgProtocolStream.gotMessage(...)  calls onColumns(desc), onRow(rows)
    │
    ▼
PgConnection collects rows into PgResultSet
    │
    ▼
CompletableFuture<ResultSet> completes  [→ application code]
    │
    ├─ connection auto-closes or returns to pool
```

### Module Responsibilities

| Module | Responsibility |
|--------|-----------------|
| `com.pgasync` (public API) | Interfaces only; contract with consumers |
| `PgConnectible` | Abstract base: shares logic between pool and plain modes |
| `PgDatabase` | Single connection per query; reconnects for each operation |
| `PgConnectionPool` | Resource pool; fair queuing; statement cache per connection |
| `PgConnection` | Stateful query executor; owns lifecycle of one `ProtocolStream` |
| `PgProtocolStream` | Message sequencing logic; buffers in-flight callbacks |
| `ProtocolStream` (interface) | Contract for network I/O; implemented by Netty or custom |
| `Message` hierarchy | Immutable DTOs; wire protocol representation |
| `Encoder`/`Decoder` | Byte ↔ Message translation; PostgreSQL binary format |
| `DataConverter` | Converts PostgreSQL OID + byte[] to/from Java types |

---

## 3. Core Concepts

### 3.1 Connection Lifecycle

```
                           PgDatabase (single conn)
                          /
Connectible ─ PgConnectible ─ getConnection()
                          \
                           PgConnectionPool (managed pool)

For each getConnection():

1. [PgDatabase only]
   ProtocolStream created
   ↓
2. stream.connect(StartupMessage)  [PostgreSQL handshake]
   ↓
3. stream.authenticate(user, password)  [MD5 or SCRAM-SHA-256]
   ↓
4. [optional] validation query runs
   ↓
5. Connection ready
   ├─ PgDatabase: new PgConnection per query
   └─ PgConnectionPool: PgConnection reused; queued if busy

Cleanup:
├─ Statements evicted from per-connection cache (LRU)
├─ Transaction rolled back on error (auto-rollback)
└─ Connection closed or returned to pool
```

**Critical invariant**: A `PgConnection` executes only ONE query at a time. PostgreSQL backend processes are single-threaded.

### 3.2 Query Execution Pipeline

#### Simple Query (all-in-one)

```
completeQuery("SELECT * FROM t WHERE id=$1", 5)
  ↓
PgConnection.query()
  ↓
Parse (compile on server)  ╌╌╌ stream.send(Parse)
RowDescription received     ╌╌╌ onColumns callback
  ↓
Bind + Describe + Execute  ╌╌╌ stream.send(Bind, Describe, Execute, SYNC)
  ↓
[loop] DataRow received    ╌╌╌ onRow callback per row
  ↓
CommandComplete received   ╌╌╌ affected rows count
ReadyForQuery received     ╌╌╌ close statement, return connection
  ↓
CompletableFuture completes with PgResultSet
```

#### Prepared Statement Reuse

```
connection.prepareStatement(sql, Oid.INT8)
  ↓
Parse(name="s-1", sql, types)  ╌╌╌ wire → server
ParseComplete received         ╌╌╌ statement cached server-side
  ↓
Connection owns PgPreparedStatement(name="s-1")
  ↓
ps.query(42)  [reuse, no re-parse]
  ↓
Bind(name="s-1", params)
Execute  ╌╌╌ skip Parse step; directly bind & execute
  ↓
Rows + CommandComplete
  ↓
ps.close() sends Close(statement)  ╌╌╌ cleanup on server
```

### 3.3 Transaction Semantics

```
connection.begin()  →  CompletableFuture<Transaction>
  ↓
sends "BEGIN" to server
  ↓
PgConnectionTransaction created (wraps connection)
  ↓
[operations inside transaction]
  ├─ tx.query(...)  [any query, auto-rolled back on error]
  ├─ tx.begin()     [nested: SAVEPOINT sp_N]
  └─ nested.begin() [deeper nesting]
  ↓
tx.commit()  →  COMMIT | RELEASE SAVEPOINT
tx.rollback()→  ROLLBACK | ROLLBACK TO SAVEPOINT
tx.close()   →  COMMIT (or ROLLBACK on exception)
```

**Key**: On any error during transaction, auto-rollback happens.

### 3.4 Connection Pool Internals

```
PgConnectionPool(maxConnections=10, maxStatements=100)
  ↓
Internal state:
  • availableConnections: Queue<PgConnection>  [idle]
  • pendingRequests: Queue<CompletableFuture>  [waiting]
  • allConnections: Set<PgConnection>  [tracking]
  • lock: ReentrantLock  [guards state]
  ↓
getConnection():
  1. Acquire lock
  2. If availableConnections.size() > 0: pop & return
  3. Else if allConnections.size() < maxConnections: create new
  4. Else: queue CompletableFuture, release lock, wait
  ↓
Each connection maintains per-connection LRU statement cache
```

**Thread-safety**: Pool uses `ReentrantLock`. Callbacks run on `Executor`, not I/O thread.

### 3.5 Message Sequencing

```
PgProtocolStream maintains:
  • onResponse: CompletableFuture waiting for next server message
  • onColumns, onRow, onAffected: callbacks for query results
  • subscriptions: Map<channel, Set<onNotification>>
  • lastSentMessage: tracks Query vs. Bind/Describe/Execute
  ↓
Invariant: ONE round-trip in flight at a time.
  - send(msg) fails with "simultaneous use detected" if concurrent
  ↓
Server messages arrive async from network layer:
  ├─ Parse/Bind response → stored in readyForQueryPendingMessage
  ├─ RowDescription → call onColumns()
  ├─ DataRow → call onRow()
  ├─ CommandComplete → call onAffected()
  ├─ ReadyForQuery → complete pending onResponse future
  ├─ ErrorResponse → fail onResponse future
  └─ NotificationResponse → publish to subscriptions
```

---

## 4. Critical Files & Entry Points

### Entry Points

| Class | Method | Purpose |
|-------|--------|---------|
| `ConnectibleBuilder` | `pool()` / `plain()` | Create pool or single-connection |
| `Connectible` | `completeQuery(sql, params)` | Execute query, get `CompletableFuture<ResultSet>` |
| `Connectible` | `completeScript(sql)` | Execute multi-statement script |
| `Connection` | `begin()` | Start transaction |
| `Connection` | `prepareStatement(sql, types)` | Parse & prepare server-side |

### Key Classes

| Class | File | Role |
|-------|------|------|
| `PgConnectible` | `PgConnectible.java` | Abstract base; routing |
| `PgDatabase` | `PgDatabase.java` | Single-connection mode |
| `PgConnectionPool` | `PgConnectionPool.java` | Pool mode; fair queuing |
| `PgConnection` | `PgConnection.java` | Active connection; queries |
| `PgProtocolStream` | `PgProtocolStream.java` | Message sequencing (NOT network I/O) |
| `ProtocolStream` | `ProtocolStream.java` | Interface; network adapter |
| `DataConverter` | `conversion/DataConverter.java` | OID ↔ Java mapping |

### Message Classes

**Frontend** (client → server): `Query`, `Parse`, `Bind`, `Describe`, `Execute`, `Close`, SASL messages  
**Backend** (server → client): `Authentication`, `RowDescription`, `DataRow`, `CommandComplete`, `ReadyForQuery`, `ErrorResponse`, `NotificationResponse`

---

## 5. Concurrency Model

### Threading Model

- **Network I/O (ProtocolStream)**: Runs on NIO event loop; MUST NEVER BLOCK
- **Future Callbacks**: Executed on `Executor` (default: `ForkJoinPool.commonPool()`)
- **Application Code**: OK to block in futures; use `.thenAcceptAsync()` if needed

### Thread-Safety Guarantees

| Component | Thread-Safe? | Notes |
|-----------|--------------|-------|
| `PgConnectionPool` | ✅ Yes | `ReentrantLock` guards state |
| `PgConnection` | ⚠️ Partial | No concurrent queries; pool ensures sequential |
| `PgProtocolStream` | ✅ Yes | Rejects concurrent calls by design |
| `Message` objects | ✅ Yes | Immutable |
| `Connectible` interface | ✅ Yes | All operations concurrent-safe |

### Query Concurrency

**Per-Connection**: Strictly sequential. Cannot pipeline.
```
PgConnection conn = ...;
conn.query(...)  // OK
  .thenCompose(rs → conn.query(...))  // OK (sequential)
  
// FAILS with "simultaneous use detected":
CompletableFuture.allOf(
  conn.query(...),
  conn.query(...)
)
```

**Per-Pool**: Fully concurrent up to `maxConnections`.

### Blocking Calls: When/Where

❌ **DO NOT** on NIO event loop: `Thread.sleep()`, `join()`, `get()`, file I/O, locks  
✅ **SAFE** in application executor or user threads: all blocking operations

---

## 6. Coding Guidelines for Agents

### Adding a New Query Type

1. Understand message flow: simple (`Query`) vs. extended (`Parse`+`Bind`+`Execute`)
2. Add message class if new (extend `Message`, immutable fields)
3. Route in `PgConnection`: implement method, call `stream.send(...)`
4. Return `CompletableFuture<?>` result
5. Test with embedded PostgreSQL

### Extending Type Conversion

1. Add OID constant to `Oid.java`
2. Implement conversion in `conversion/` package
3. Register in `DataConverter` or implement `Converter<T>` interface
4. Test in `TypeConverterTest`

### Implementing Custom ProtocolStream

1. Extend `PgProtocolStream`; override `write(Message...)`, `isConnected()`, `close()`
2. In your NIO read handler: `Decoder.decode(bytes)` → `this.gotMessage(msg)`
3. Subclass `ConnectibleBuilder`; override `pool()` / `plain()`

### Error Handling Pattern

```java
pool.completeQuery(sql)
    .handle((rs, th) → {
        if (th instanceof SqlException) { ... }
        else if (th instanceof IllegalStateException) { ... }
        else { ... }
    })
```

### Anti-Patterns

❌ DO NOT: reuse futures, parallel queries on same connection, block event loop, leave trans open indefinitely, assume statement names stable  
✅ DO: chain with `.thenCompose()`, use `getConnection()` → `begin()` → queries → `commit()`, set `maxConnections` wisely

---

## 7. Testing Strategy

### Framework

- **JUnit 4**: `@ClassRule`, `@Rule`
- **Embedded PostgreSQL**: auto-downloads binary on first run
- **Netty**: reference `ProtocolStream` in test scope

### Running Tests

```powershell
./gradlew test
./gradlew test --tests "com.github.pgasync.QueryResultTest"
./gradlew test --tests "*Pooling*"
```

### Test Coverage Areas

| Test | Covers |
|------|--------|
| `PlainConnectionTest` | Single-connection queries |
| `ConnectionPoolingTest` | Pool behavior, queueing |
| `QueryResultTest` | Result set mapping |
| `ParametersBindingTest` | Parameter binding |
| `TransactionTest` | BEGIN/COMMIT/ROLLBACK |
| `AuthenticationTest` | MD5, SCRAM-SHA-256 |
| `ListenNotifyTest` | LISTEN/NOTIFY |
| `TypeConverterTest` | Type round-trips |
| `ArrayConversionsTest` | Array handling |
| `IOTest` | Wire format encode/decode |

### Test Pattern

```java
@ClassRule
public static DatabaseRule db = new DatabaseRule();

@Test
public void myTest() {
    db.grantConnectionAsync(connBuilder → 
        connBuilder.plain()
            .completeQuery("SELECT 1")
            .thenAccept(rs → { ... })
    );
}
```

---

## 8. Common Pitfalls

### 1. Simultaneous Use Detected

```
Cause: Sending 2 messages before first response arrives
Fix: Chain with .thenCompose() or use different connections
```

### 2. Statement Cache Thrashing

```
Cause: Many distinct SQL strings (each a unique statement)
Fix: Parameterize queries; tune maxStatements
```

### 3. Connection Leaks

```
Cause: Query fails in pool; connection not returned
Fix: completeQuery() auto-closes; exceptions propagate safely
```

### 4. Blocking Event Loop

```
Cause: Thread.sleep() / join() in callback on NIO thread
Fix: Use .thenAcceptAsync(action, customExecutor)
```

### 5. Type Conversion Mismatches

```
Cause: OID doesn't match Java type converter
Fix: Use row.getValue("col") generic access or check type
```

### 6. Array NULL Handling

```
PostgreSQL: ARRAY[1, NULL, 3] ≠ ARRAY[] ≠ NULL
Java: Distinct null vs. empty list values
```

### 7. Password Encoding

```
Ensure UTF-8; SCRAM-SHA-256 applies RFC 4013 SASLprep rules
```

---

## 9. Example Workflows

### Basic Query

```java
Connectible pool = new NettyConnectibleBuilder()
    .hostname("localhost").port(5432)
    .username("user").password("pass").database("mydb")
    .pool();

pool.completeQuery("SELECT id, name FROM users WHERE id = $1", 42)
    .thenAccept(rs → {
        Row row = rs.at(0);
        System.out.printf("User: %s\n", row.getString("name"));
    });
```

### Transaction with Nested Savepoint

```java
pool.getConnection()
    .thenCompose(conn → conn.begin()
        .thenCompose(tx → tx.completeQuery("UPDATE users SET balance = balance - $1 WHERE id = $2", 100, user1)
            .thenCompose(rs1 → tx.begin()
                .thenCompose(nested → nested.completeQuery("INSERT INTO log VALUES ($1)", msg)
                    .thenCompose(rs2 → nested.commit())
                )
            )
            .thenCompose(v → tx.commit())
        )
    );
```

### Prepared Statement Reuse

```java
conn.prepareStatement("INSERT INTO logs (msg) VALUES ($1)", Oid.VARCHAR)
    .thenCompose(stmt →
        CompletableFuture.allOf(
            IntStream.range(0, 100)
                .mapToObj(i → stmt.query("Log message " + i))
                .toArray(CompletableFuture[]::new)
        )
        .thenCompose(v → stmt.close())
    );
```

### Custom Type Converter

```java
public class MoneyConverter implements Converter<Money> {
    @Override public Class<Money> type() { return Money.class; }
    @Override public byte[] convert(Money m) { return m.toString().getBytes(UTF_8); }
}

Connectible pool = new NettyConnectibleBuilder()
    .hostname("localhost")
    .converters(new MoneyConverter())
    .pool();

pool.completeQuery("SELECT price FROM items WHERE id = $1", 1)
    .thenAccept(rs → Money price = rs.at(0).getValue("price", Money.class));
```

---

## 10. Repository Layout

```
src/
  main/java/
    com/pgasync/           # Public API (interfaces only)
    com/github/pgasync/    # Core implementation
      conversion/          # Type converters
      io/                  # Encoder/Decoder
        backend/           # Message decoders
        frontend/          # Message encoders
      message/             # Message DTOs
      sasl/                # SASL/SASLprep
  test/java/
    com/github/pgasync/    # Integration tests
      netty/               # Netty implementation
      sasl/                # SASL unit tests
```

---

## 11. Dependencies

**Implementation** (runtime):
- `javax.xml.bind:jaxb-api:2.3.1`

**Test Only**:
- `io.netty:netty-handler:4.1.90.Final`
- `junit:junit:4.13.2`
- `ru.yandex.qatools.embed:postgresql-embedded:2.10`

**Zero blocking dependencies**: Core library has NO runtime dependency on Netty or any NIO framework.

---

## Quick Reference

### Build Commands

```powershell
./gradlew build          # Full build + test
./gradlew test           # Run all tests
./gradlew classes        # Compile only
```

### Common Errors

| Error | Fix |
|-------|-----|
| `simultaneous use detected` | Chain with `.thenCompose()` |
| `CompletionException` | Check `.getCause()` for SqlException |
| Pool exhausted | Increase `maxConnections` |
| `AuthException` | Verify credentials; check PG config |

### Public API Interfaces

- `Connectible`: Base; `query()`, `getConnection()`
- `Connection`: Adds `begin()`, `close()`
- `Transaction`: Adds `commit()`, `rollback()`
- `ResultSet`: `size()`, `at()`, `affectedRows()`
- `Row`: `getValue()`, `getString()`, `getInt()`, …
- `Converter<T>`: Extension point for custom types

---

## Summary

**postgres-async-driver** is a low-level, high-performance async PostgreSQL driver for non-blocking database operations. Key principles:

1. **Async-first**: All operations return `CompletableFuture`
2. **Protocol-aware**: Direct PostgreSQL v3 wire protocol
3. **Pluggable I/O**: Framework-agnostic network layer
4. **Type-safe**: Extensible type converters
5. **Connection pooling**: Fair queueing, statement caching
6. **Simplicity + Power**: High-level convenience + low-level control

Perfect for microservices, event-driven systems, and reactive pipelines.

---

**Project Metadata**  
- **Version**: 1.0.5
- **Group/Artifact**: `com.github.pgasync:postgres-async-driver`
- **License**: Apache 2.0
- **JDK**: 11+
- **Build**: Gradle (Kotlin DSL)
