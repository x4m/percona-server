#!/bin/bash
cd "$(dirname "$0")"
# MYSQLD: a build of the 8.4 branch with all seven fixes applied; bin-unpatched: plain 8.4
export MYSQLD=~/bin-allfixes/bin/mysqld
python3 harness.py --fresh --iters 30 --modes garbage-data --min-run 4 --max-run 12 > exp-garbage-data.log 2>&1
python3 harness.py --fresh --iters 20 --modes garbage --min-run 4 --max-run 12 > exp-garbage.log 2>&1
python3 harness.py --fresh --iters 20 --modes stale --min-run 4 --max-run 12 --opt innodb_flush_method=fsync > exp-stale-fsync.log 2>&1
python3 harness.py --fresh --iters 10 --modes stale --min-run 4 --max-run 12 --opt innodb_flush_method=O_DIRECT_NO_FSYNC > exp-stale-nofsync.log 2>&1
MYSQLD=~/bin-unpatched/bin/mysqld python3 harness.py --fresh --iters 40 --modes stale --min-run 4 --max-run 12 > exp-stale-unpatched.log 2>&1
echo ALL-DONE > exp-done
