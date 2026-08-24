# Replication Slots

A replication slot is a piece of named, persistent server-side state: a directory under the `pg_replslot/<slot_name>` containing a state file with maybe a dozen values. It's not a process, not a connection, not a queue, and it holds none of your data. It survives restarts, and it exists whether or not anyone is connected to it.

- The values that matter:
    - `restart_lsn`: Oldest WAL position the server must keep. This is what pinks WAL.
    - `confirmed_flush_lsn`: How far the consumer has confirmed durable processing. Where you resume.
    - `catalog_xmin`: Oldest transaction ID whose catalog rows must be preserved.
    - `active`/`active_pid`: Whether a consumer is attached right now.
    - `plugin`/`slot_type`/`database`: `pgoutput`, `logical`, and which DB it's bound to.

