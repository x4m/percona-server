# Power-loss harness for InnoDB crash recovery

Not for merge. This is the fault-injection harness behind the crash
recovery fixes proposed in x4m/percona-server PRs #1-#7; it is published so
that the failures they describe can be reproduced and the traces in the PRs
checked. Linux only.

## What it does

`mysqld` runs under an `LD_PRELOAD` shim (`plshim.c`) that journals every
write, fsync, truncate, unlink and rename under the data directory and saves
the pre-image of every written byte range. A simulated power loss is:
freeze the process (SIGSTOP), read the journal, kill it, then for every
512-byte sector with writes not yet covered by an fsync of that file
(or an `O_SYNC`/`O_DSYNC` fd) pick one of

| loss model | sector after the "power loss" |
|---|---|
| `keep` | the newest write reached the disk |
| `old` | rolled back to the last durable image, or an intermediate one |
| `garbage` | random bytes (a torn sector) |

`--modes` selects how the pick is made: `stale` (keep/old only: a device
that acknowledges writes from a volatile cache and loses power), `garbage`
(also torn sectors in every file), `garbage-data` (torn sectors in data
files, redo files stay sector-atomic), `mixed`.

Writes to an `O_DIRECT` fd are *not* treated as durable: O_DIRECT bypasses
the page cache, not the device write cache. That is the assumption behind
PR #2; `innodb_flush_method=fsync` and `O_DIRECT_NO_FSYNC` are the controls.

The workload (`harness.py`, 8 clients by default) is a ledger with a
kv table updated in the same transaction, a FULLTEXT index, a BLOB table,
and a DDL thread (CREATE/DROP/TRUNCATE/ALTER/RENAME) with
`innodb_undo_log_truncate=ON`. Every committed transaction is remembered by
the client. After the crash the server is restarted and checked: all
acknowledged rows present, `kv.v == 1000 + SUM(ledger.delta)` per key,
secondary index counts equal to the PK count, FULLTEXT `MATCH` count equal
to the row count, `CHECK TABLE`.

Two checkers run on every journal, independently of whether the server
restarts:

- `walcheck.py`: every data page written has `FIL_PAGE_LSN <= ` the redo
  LSN durable at the time of the write (the WAL rule);
- `dblwrcheck.py`: (A) every data page write is preceded by an fsynced
  doublewrite copy of the same (space, page, LSN); (B) no doublewrite slot
  is overwritten while a page it protects is written but not yet fsynced
  (a slot of a tablespace that is dropped or truncated later in the run is
  reported separately as benign).

The journal is written under a mutex before the write is issued and after
the fsync returns, so its order is causal and the checkers need no
timestamps. Rule A is exactly the ordering PR #2 restores; the failing trace
is in `traces/dblwr-fsync-gap.txt`.

## Files

| file | |
|---|---|
| `plshim.c` | LD_PRELOAD shim; journal format in the header comment |
| `pl.py` | loss models; `crash()` freezes, kills and applies the loss |
| `harness.py` | workload, crash loop, restart, verification, artifacts |
| `walcheck.py`, `dblwrcheck.py` | journal checkers (also runnable standalone on a `journal.log`) |
| `run_experiments.sh` | the runs reported in the PRs |
| `traces/` | journal excerpts referenced from the PRs |

## Running

```sh
gcc -O2 -shared -fPIC -o plshim.so plshim.c -ldl -lpthread
pip install pymysql

# PS_BUILD: cmake build tree (share/, plugin_output_directory/); MYSQLD: the
# binary to test, default $PS_BUILD/bin/mysqld
export PS_BUILD=~/percona-server/build
python3 harness.py --fresh --iters 20 --modes stale --min-run 4 --max-run 12
python3 harness.py --fresh --iters 20 --modes stale --opt innodb_flush_method=fsync
```

Each iteration prints the journal statistics, the loss applied, the checker
verdicts and the verification result. When the server does not come back or
verification fails, `artifacts/<time>-<tag>/` gets `journal.log.last` (the
journal up to the crash), `undo.bin`, `mysqld.err`, `my.cnf`, `pre.tar`
(the data directory after the loss, before recovery ran) and `data.tgz`.
`journal.log.last` is what the traces in the PRs are cut from; a journal
can be re-checked with `python3 dblwrcheck.py <journal>`.

The data directory needs a filesystem that supports O_DIRECT (not tmpfs).
One iteration is 4-12 s of workload plus recovery; 20 iterations take
about 5 minutes.

## Results behind the PRs

Percona Server 8.4.11-11 (`8.4` at 37cc047c), defaults unless noted.

| build | `--modes` | `innodb_flush_method` | crashes | server did not restart | other failures |
|---|---|---|---|---|---|
| 8.4 | stale | O_DIRECT (default) | 40 | 11 | 20 (lost committed FULLTEXT rows, torn TRX_SYS page, ...) |
| 8.4 | stale | fsync | 20 | 0 | 0 |
| 8.4 | stale | O_DIRECT_NO_FSYNC | 10 | 10 | (expected: the documented no-fsync contract) |
| all 7 fixes | stale | O_DIRECT | 60 | 0 | 0 |
| all 7 fixes | garbage | O_DIRECT | 20 | 0 | 0 |
| all 7 fixes | garbage-data | O_DIRECT | 30 | 0 | 0 |
| all 7 fixes | stale,garbage-data | O_DIRECT | 30 | 2 | see below |

Rule A (`dblwrcheck.py`) fails on every 8.4/O_DIRECT journal: no
`.dblwr` file is ever fsynced. It passes on every journal with PR #2 or
with `innodb_flush_method=fsync`. The WAL rule never failed.

The two remaining failures are an open issue not covered by the PRs: in
`garbage-data` mode a FULLTEXT auxiliary tablespace whose first sector of
page 0 is torn cannot have its space id determined by the directory scan
(`Fil_system::get_tablespace_id()` reads only that sector), and the file
is skipped instead of being restored from the doublewrite buffer.

Analysis and code were AI-assisted.
