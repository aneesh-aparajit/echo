# WAL Replication

- **short version**: we are building a postgres logical replication client. postgres already does the hard part. it stores the wal into logical row changes. echo will simply read these rows and publish to a sink.

- **the pieces**:
    - WAL -> logical decoding -> output plugin -> replication slot -> consumer
    - **replication slot**: it is a server-side state that remembers how far we have consumed. postgres will retain the WAL segments until you confirm you've flushed past them. this provides us with durability across restarts.

## WAL

### Why it exists

- WAL is the single most important mechanism in Postgres.
- The naive way to make a database durable is to fsync every modified page to commit. That's brutally slow: pages are scattered cross the file, so you'd do random 8kB writes and wait for the disk on every transaction.
- WAL inverts this, before any change is commited to a page on disk, a description of that change is appended to a sequential log and flushed. Then - and only then - is the transaction is allowed to report success. The actual data pages stay dirty in shared buffers and get written out later, lazily by the background writer or checkpointer.
- This gives two wins. Commits become **sequential appends** to one file instead of random scattered writes. And crash recovery becomes possible: if the server dies with dirty pages unwritten, replaying the log from the last checkpoint reconstructs them.
- The invariant that makes it work is the **write-ahead rule**: a dirty page may never reach disk, before the WAL record describing it's modifications have been flushed. Postgres enforces this by stamping every data page with the LSN of the last WAL record that touched it (`pd_lsn` in the page header), and checking the LSN before evicting.
    - This is the same principle of ARIES, the same design pattern most databases use.
        - **ARIES**: Algorithms for Recovory and Isolation Exploiting Semantics
            - Three principles behind ARIES:
                - **Write-Ahead-Logging**: Any write must be written to a log, and the log most be persisted to a durable storage, before actually committing the dirty buffer to the page.
                - **Repeating history during Redo**: On recovery from a crash, ARIES retraces the actions of the database from the last checkpointed version and restores the state right before the crash.
                - **Logging changes of Undo**: When a transaction fails, even the undo operations are logged, so during the recovery process, so that the changes are undone, for consistency.

### LSN: the address space

- An **LSN** (Log Sequence Number) is an `uint64` representing the byte offset into a single conceptual, infinite WAL stream. It's the coordinate system - every WAL record has one, every data page records one, replication slots store one, `pg_state_replication` reports lag in terms of them.
- It's printed as two hex halves: `0/16B3748`, essentially `high/low` both of 32 bits.
- Since it's just a byte offset, LSNs are directly comparable and subtractable - `pg_wal_lsn_diff()` literally gives you a byte count. That's why slot lag is measured in bytes.

### Physical layout: files, pages, records

- Three levels of nesting:
    - pg_wal -> segments -> pages -> records

```plaintext
pg_wal/
├── 000000010000000000000001   ← segment, 16MB
├── 000000010000000000000002
└── 000000010000000000000003

each segment:
┌──────────┬──────────┬──────────┬─────
│  page 0  │  page 1  │  page 2  │ ...     ← 8KB each (XLOG_BLCKSZ)
└──────────┴──────────┴──────────┴─────

each page:
┌────────────┬────────┬────────┬────────┬──────
│ pageheader │ record │ record │ record │ ...
└────────────┴────────┴────────┴────────┴──────
```

- **Segments** are 16MB by default (settable at `initdb` via `--wal-segsize`). The 24-character filename is three 8-hex-digit fields: timeline, log id, segment number
    - **timeline** increments on every point-in-time recovery or promotion - it's how Postgres keeps divergent histories from colliding. Segments are recycled. by renaming, not deleting so the filenames you see are reused files with fresh contents.

- **Pages** are 8KB and each carries a header:
    - The first page of the segment gets a long header `xlp_sysid` (the cluster identifier), `xlp_seg_size`, `xlp_xlog_blcksz` - a sanity check so you don't accidentally feed one cluster's WAL to another, or WAL built with different compile-time block sizes.
    - `xlp_rem_len` handles the fact that records don't respect page boundaries. A record can start near the end of the page and spill into the next page; the continuation page sets `XLP_FIRST_IS_CONTRECORD` in `xlp_info` and puts the remaining count in `xlp_rem_len`. Any reader has to reassemble these.

```c
typedef struct XLogPageHeaderData {
    uint16      xlp_magic;      // version identifier
    uint16      xlp_info;       // flag bits
    TimeLineID  xlp_tli; 
    XLogRecPtr  xlp_pageaddr;   // LSN of this page's first byte
    uint32      xlp_rem_len;    // 20 bytes, MAXALIGNed to 24
} XLogPageHeaderData;
```

### Record Structure

```c
typedef struct XLogRecord {
    uint32          xl_tot_len;     // total len of the entire record.
    TransactionId   xl_xid;         // transaction id
    XLogRecPtr      xl_prev;        // LSN of the previous record;
    uint8           xl_info;        // rmgr-specific flag bits
    RmgrId          xl_rmid;        // resource manager id
    pg_crc32c       xl_crc;         // crc-32c of the rest of the record
} XLogRecord;
```

- `xl_prev` makes the log a backward-linked list, which lets a reader validate that it's on a real record boundary rather than reading from recycled segment. `xl_crc` catches torn or corrupt records - critical at the tail of the log after a crash, where the last record may be half-written.
- `xl_rmid` is the dispatch key. Postgres doesn't have one WAL format; it has **resource managers** each owning its own types and replay logic:

|rmid|resource_manager|what it logs|
|-|-|-|
|0|XLOG|checkpoit, FPIs, switches|
|1|Transaction|commit,abort,prepare|
|2|Storage|relation create/drop/truncate|
|10|Heap2|multi-insert, freeze, visibility|
|11|Heap|insert, update, delete, lock|
|12|Btree|index changes|
|16|Standby|locks, running-xacts snapshots|
|21|LogicalMessage|`pg_logical_emit_message()`|

...there are many more. Each registers a `rm_redo` callback for recovery and ` rm_desc` and `pg_waldump`. Extensions can add custom ones.

- **Logical Decoding** cares only about a handful of these: Heap, Heap2, Transaction, Standby, LogicalMessage, XLOG. Everything else (index updates, freezing) is physical noise that gets skipped, because indexes are derived data. This is only for the **Echo** project, because we only care about the data itself.

- After the header comes a variable-length payload built from tagged chuncks:

```plaintext
┌────────────────┬─────────────────┬─────────────────┬──────────────┐
│ XLogRecord hdr │ block ref 0     │ block ref 1     │ main data    │
│    24 bytes    │ (+ optional     │                 │              │
│                │  page image)    │                 │              │
└────────────────┴─────────────────┴─────────────────┴──────────────┘
```

- Each block reference starts with:

```c
typedef struct XLogRecordBlockHeader {
    uint8   id;             /* block reference ID, 0..N */
    uint8   fork_flags;     /* which fork + flags */
    uint16  data_length;     /* payload bytes, excluding page image */
} XLogRecordBlockHeader;
```
