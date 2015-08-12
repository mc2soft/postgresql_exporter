# PostgreSQL Server Exporter

Prometheus exporter for PostgreSQL server metrics. Supported PostgreSQL versions: 9.0 and up.


## Build and run

You need latest version of go to build.

    go build
    export DATA_SOURCE_NAME='user=username dbname=database password=password sslmode=disable'
    ./postgresql_exporter <flags>

### Flags

Name                    | Description
------------------------|------------
web.listen-address      | Address to listen on for web interface and telemetry.
web.telemetry-path      | Path under which to expose metrics.
db.names                | Comma-separated list of monitored DB.
db.consider-query-slow  | Queries with execution time higher than this value will be considered as slow (in seconds). 5 seconds by default.
db.tables               | Comma-separated list of tables to track.

### Data source name

The PostgreSQL [data source name](http://en.wikipedia.org/wiki/Data_source_name)
must be set via the `DATA_SOURCE_NAME` environment variable.
Format and available parameters is described at http://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters

## Stats

Exporter will send following stats to prometheus

### Buffers

* `buffers_checkpoint`    - Number of buffers written during checkpoints
* `buffers_clean`         - Number of buffers written by the background writer
* `maxwritten_clean`      - Number of times the background writer stopped a cleaning scan because it had written too many buffers
* `buffers_backend`       - Number of buffers written directly by a backend
* `buffers_backend_fsync` - Number of times a backend had to execute its own fsync call (normally the background writer handles those even when the backend does its own write)
* `buffers_alloc`         - Number of buffers allocated

### Database

* `numbackends`     - Number of backends currently connected to this database
* `tup_returned`    - Number of rows returned by queries in this database
* `tup_fetched`     - Number of rows fetched by queries in this database
* `tup_inserted`    - Number of rows inserted by queries in this database
* `tup_updated`     - Number of rows updated by queries in this database
* `tup_deleted`     - Number of rows deleted by queries in this database
* `xact_commit`     - Number of transactions in this database that have been committed
* `xact_rollback`   - Number of transactions in this database that have been rolled back
* `deadlocks`       - Number of deadlocks detected in this database
* `temp_files`      - Number of temporary files created by queries in this database
* `temp_bytes`      - Total amount of data written to temporary files by queries in this database
* `db_size`         - Database size
* `cache_hit_ratio` - Database cache hit ratio

### Tables

* `seq_scan`              - Number of sequential scans initiated on this table
* `seq_tup_read`          - Number of live rows fetched by sequential scans
* `vacuum_count`          - Number of times this table has been manually vacuumed (not counting VACUUM FULL)
* `autovacuum_count`      - Number of times this table has been vacuumed by the autovacuum daemon
* `analyze_count`         - Number of times this table has been manually analyzed
* `autoanalyze_count`     - Number of times this table has been analyzed by the autovacuum daemon
* `n_tup_ins`             - Number of rows inserted
* `n_tup_upd`             - Number of rows updated
* `n_tup_del`             - Number of rows deleted
* `n_tup_hot_upd`         - Number of rows HOT updated (i.e., with no separate index update required)
* `n_live_tup`            - Estimated number of live rows
* `n_dead_tup`            - Estimated number of dead rows
* `table_cache_hit_ratio` - Table cache hit ration in percents
* `table_items_count`     - Table overall items count

### Slow queries

* `slow_queries`        - Number of slow queries
* `slow_select_queries` - Number of slow SELECT queries
* `slow_dml_queries`    - Number of slow data manipulation queries (INSERT, UPDATE, DELETE)

