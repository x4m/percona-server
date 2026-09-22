#!/usr/bin/env python3
"""Crash-consistency harness for InnoDB under a simulated power loss.

Loop: start mysqld under plshim -> run DML+DDL workload -> freeze, kill,
apply loss model to non-durable writes -> restart -> verify.
"""
import os, sys, time, random, subprocess, threading, shutil, json, argparse, glob
import pymysql
import pl

HERE = os.path.dirname(os.path.abspath(__file__))
# Build tree of the server under test (share/, plugin_output_directory/);
# MYSQLD may point at another binary built from the same sources.
BUILD = os.environ.get('PS_BUILD', os.path.expanduser('~/percona-server/build'))
MYSQLD = os.environ.get('MYSQLD', f'{BUILD}/bin/mysqld')
SHIM = f'{HERE}/plshim.so'
WORK = f'{HERE}/work'
ART = f'{HERE}/artifacts'
SOCK = f'{WORK}/mysql.sock'
DATADIR = f'{WORK}/data'
ERRLOG = f'{WORK}/mysqld.err'
JOURNAL = f'{WORK}/journal.log'
UNDO = f'{WORK}/undo.bin'
PORT = 3307
NKV = 200


def log(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)


def cnf(opts):
    base = f"""[mysqld]
basedir={BUILD}
datadir={DATADIR}
lc-messages-dir={BUILD}/share
plugin-dir={BUILD}/plugin_output_directory
port={PORT}
socket={SOCK}
pid-file={WORK}/mysqld.pid
log-error={ERRLOG}
mysqlx=OFF
skip-log-bin
skip-name-resolve
max_connections=200
innodb_use_native_aio=OFF
innodb_flush_log_at_trx_commit=1
log-error-verbosity=3
innodb_buffer_pool_size=64M
innodb_redo_log_capacity=64M
innodb_undo_log_truncate=ON
innodb_max_undo_log_size=16M
innodb_purge_rseg_truncate_frequency=1
innodb_page_cleaners=2
innodb_lru_scan_depth=256
innodb_adaptive_flushing=ON
innodb_io_capacity=2000
innodb_print_all_deadlocks=OFF
innodb_fast_shutdown=0
"""
    for k, v in opts.items():
        base += f'{k}={v}\n'
    return base


def write_cnf(opts):
    with open(f'{WORK}/my.cnf', 'w') as f:
        f.write(cnf(opts))


def init_datadir(opts):
    shutil.rmtree(DATADIR, ignore_errors=True)
    os.makedirs(DATADIR)
    write_cnf(opts)
    r = subprocess.run([MYSQLD, f'--defaults-file={WORK}/my.cnf', '--initialize-insecure'],
                       capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout, r.stderr)
        raise SystemExit('initialize failed')


def start_mysqld(shim=True):
    env = dict(os.environ)
    if shim:
        for p in (JOURNAL, UNDO):
            if os.path.exists(p):
                os.remove(p)
        env.update(LD_PRELOAD=SHIM, PL_DIR=DATADIR, PL_LOG=JOURNAL, PL_UNDO=UNDO)
    proc = subprocess.Popen([MYSQLD, f'--defaults-file={WORK}/my.cnf'], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    t0 = time.time()
    while time.time() - t0 < 300:
        if proc.poll() is not None:
            return None, time.time() - t0
        try:
            c = connect(db=None)
            c.close()
            return proc, time.time() - t0
        except Exception:
            time.sleep(0.2)
    proc.kill()
    return None, time.time() - t0


def connect(db='test', autocommit=False):
    return pymysql.connect(unix_socket=SOCK, user='root', database=db, autocommit=autocommit,
                           read_timeout=600, write_timeout=600)


def stop_mysqld(proc):
    try:
        c = connect(autocommit=True)
        c.cursor().execute('SHUTDOWN')
        c.close()
    except Exception:
        pass
    try:
        proc.wait(timeout=300)
    except Exception:
        proc.kill()


SCHEMA = f"""
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE TABLE IF NOT EXISTS kv (k INT PRIMARY KEY, v BIGINT NOT NULL, pad VARCHAR(200), KEY iv (v)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ledger (id BIGINT AUTO_INCREMENT PRIMARY KEY, client INT NOT NULL, seq INT NOT NULL,
  k INT NOT NULL, delta INT NOT NULL, payload VARCHAR(300) NOT NULL,
  UNIQUE KEY cs (client, seq), KEY ik (k), FULLTEXT ft (payload)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS scratch (id INT AUTO_INCREMENT PRIMARY KEY, b BLOB, KEY ib (b(32))) ENGINE=InnoDB;
"""


def setup_schema():
    c = connect(db=None, autocommit=True)
    cur = c.cursor()
    for stmt in SCHEMA.strip().split(';'):
        if stmt.strip():
            cur.execute(stmt)
    cur.execute('SELECT COUNT(*) FROM test.kv')
    if cur.fetchone()[0] == 0:
        cur.executemany('INSERT INTO test.kv VALUES (%s, 1000, REPEAT("p", 100))', [(i,) for i in range(NKV)])
    c.close()


class Workload:
    def __init__(self, nclients, ddl, seed):
        self.nclients = nclients
        self.ddl = ddl
        self.stop = threading.Event()
        self.acks = {}          # client -> set(seq)
        self.lock = threading.Lock()
        self.rng = random.Random(seed)
        self.threads = []
        self.errors = []
        self.ntxn = 0
        self.nddl = 0

    def start(self, next_seq):
        for cid in range(self.nclients):
            self.acks[cid] = set()
            t = threading.Thread(target=self.client, args=(cid, next_seq.get(cid, 1)), daemon=True)
            t.start()
            self.threads.append(t)
        if self.ddl:
            t = threading.Thread(target=self.ddl_thread, daemon=True)
            t.start()
            self.threads.append(t)

    def client(self, cid, seq):
        rng = random.Random(cid * 7919 + self.rng.random())
        conn = None
        while not self.stop.is_set():
            try:
                if conn is None:
                    conn = connect()
                cur = conn.cursor()
                cur.execute('BEGIN')
                n = rng.randint(1, 3)
                for _ in range(n):
                    k = rng.randrange(NKV)
                    d = rng.randint(1, 5)
                    payload = f'tok c{cid} s{seq} k{k} ' + ' '.join(rng.choice(['apple', 'pear', 'plum', 'fig']) for _ in range(rng.randint(3, 40)))
                    cur.execute('INSERT INTO ledger(client, seq, k, delta, payload) VALUES (%s,%s,%s,%s,%s)',
                                (cid, seq, k, d, payload))
                    cur.execute('UPDATE kv SET v = v + %s, pad = %s WHERE k = %s', (d, payload[:200], k))
                    seq += 1
                if rng.random() < 0.3:
                    cur.execute('INSERT INTO scratch(b) VALUES (%s)', (os.urandom(rng.randint(10, 3000)),))
                if rng.random() < 0.05:
                    cur.execute('DELETE FROM scratch WHERE id IN (SELECT id FROM (SELECT id FROM scratch ORDER BY id LIMIT 20) x)')
                conn.commit()
                with self.lock:
                    for s in range(seq - n, seq):
                        self.acks[cid].add(s)
                    self.ntxn += 1
            except pymysql.err.OperationalError as e:
                if self.stop.is_set():
                    break
                # lock wait timeout / deadlock / connection lost: retry the whole txn
                try:
                    conn.rollback()
                except Exception:
                    conn = None
                if e.args and e.args[0] in (1205, 1213):
                    continue
                time.sleep(0.05)
                conn = None
            except Exception as e:
                if self.stop.is_set():
                    break
                self.errors.append(repr(e))
                try:
                    conn.rollback()
                except Exception:
                    conn = None
                time.sleep(0.1)

    DDLS = [
        'ALTER TABLE kv ADD INDEX ipad (pad(20))',
        'ALTER TABLE kv DROP INDEX ipad',
        'ALTER TABLE ledger ADD COLUMN extra INT DEFAULT 7, ALGORITHM=INSTANT',
        'ALTER TABLE ledger DROP COLUMN extra, ALGORITHM=INSTANT',
        'ALTER TABLE ledger ADD INDEX idelta (delta), ALGORITHM=INPLACE',
        'ALTER TABLE ledger DROP INDEX idelta',
        'CREATE TABLE IF NOT EXISTS copy1 AS SELECT * FROM ledger',
        'DROP TABLE IF EXISTS copy1',
        'TRUNCATE TABLE scratch',
        'OPTIMIZE TABLE scratch',
        'ANALYZE TABLE ledger',
        'ALTER TABLE scratch ENGINE=InnoDB',
        'ALTER TABLE ledger ADD FULLTEXT ft2 (payload)',
        'ALTER TABLE ledger DROP INDEX ft2',
    ]

    def ddl_thread(self):
        rng = random.Random(self.rng.random())
        conn = None
        while not self.stop.is_set():
            time.sleep(rng.uniform(0.5, 2.0))
            try:
                if conn is None:
                    conn = connect(autocommit=True)
                cur = conn.cursor()
                cur.execute('SET SESSION lock_wait_timeout = 10')
                stmt = rng.choice(self.DDLS)
                cur.execute(stmt)
                self.nddl += 1
            except Exception as e:
                if self.stop.is_set():
                    break
                if 'Duplicate key name' in str(e) or "check that column/key exists" in str(e) or 'Duplicate column' in str(e) or "Can't DROP" in str(e):
                    continue
                if isinstance(e, pymysql.err.OperationalError) and e.args and e.args[0] in (2013, 2006, 1205):
                    conn = None
                    continue
                self.errors.append('DDL ' + repr(e))
                conn = None

    def halt(self):
        self.stop.set()


def verify(acks, prev_state):
    """Return list of problems (empty = ok)."""
    problems = []
    c = connect(autocommit=True)
    cur = c.cursor()
    # 1. all acked ledger rows are present
    cur.execute('SELECT client, seq FROM ledger')
    present = {}
    for cl, s in cur.fetchall():
        present.setdefault(cl, set()).add(s)
    for cl, seqs in acks.items():
        missing = seqs - present.get(cl, set())
        if missing:
            problems.append(f'client {cl}: {len(missing)} acked rows missing, e.g. {sorted(missing)[:5]}')
    # 2. kv/ledger atomicity: v(k) == 1000 + sum(delta)
    cur.execute('SELECT k.k, k.v, 1000 + COALESCE(SUM(l.delta),0) FROM kv k LEFT JOIN ledger l ON l.k = k.k GROUP BY k.k, k.v HAVING k.v <> 1000 + COALESCE(SUM(l.delta),0)')
    bad = cur.fetchall()
    if bad:
        problems.append(f'kv/ledger mismatch on {len(bad)} keys, e.g. {bad[:3]}')
    # 3. index consistency
    for tbl, idx in (('ledger', 'cs'), ('ledger', 'ik'), ('kv', 'iv')):
        cur.execute(f'SELECT COUNT(*) FROM {tbl} FORCE INDEX ({idx})')
        a = cur.fetchone()[0]
        cur.execute(f'SELECT COUNT(*) FROM {tbl} FORCE INDEX (PRIMARY)')
        b = cur.fetchone()[0]
        if a != b:
            problems.append(f'{tbl}: index {idx} count {a} != PK count {b}')
    cur.execute('SELECT COUNT(*) FROM ledger WHERE MATCH(payload) AGAINST ("tok")')
    a = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM ledger')
    b = cur.fetchone()[0]
    if a != b:
        problems.append(f'ledger: FULLTEXT count {a} != row count {b}')
    for tbl in ('kv', 'ledger', 'scratch'):
        cur.execute(f'CHECK TABLE {tbl} EXTENDED')
        for row in cur.fetchall():
            if row[2] == 'error' or (row[2] == 'status' and row[3] != 'OK'):
                problems.append(f'CHECK TABLE {tbl}: {row}')
    cur.execute('SELECT client, MAX(seq) FROM ledger GROUP BY client')
    next_seq = {cl: mx + 1 for cl, mx in cur.fetchall()}
    c.close()
    return problems, next_seq


def scan_errlog(since_pos):
    bad = []
    with open(ERRLOG, errors='replace') as f:
        f.seek(since_pos)
        for line in f:
            if '[ERROR]' in line or 'Assertion' in line or 'signal' in line.lower() and 'got signal' in line.lower():
                bad.append(line.rstrip())
        pos = f.tell()
    return bad, pos


def save_artifacts(tag, extra):
    d = f'{ART}/{time.strftime("%Y%m%d-%H%M%S")}-{tag}'
    os.makedirs(d)
    for p in (ERRLOG, JOURNAL, UNDO, JOURNAL + '.last', f'{WORK}/my.cnf', f'{WORK}/pre.tar'):
        if os.path.exists(p):
            shutil.copy(p, d)
    subprocess.run(['tar', 'czf', f'{d}/data.tgz', '-C', WORK, 'data'])
    with open(f'{d}/info.json', 'w') as f:
        json.dump(extra, f, indent=1, default=str)
    log('ARTIFACTS ->', d)
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--iters', type=int, default=20)
    ap.add_argument('--clients', type=int, default=8)
    ap.add_argument('--modes', default='mixed,stale,garbage')
    ap.add_argument('--min-run', type=float, default=4)
    ap.add_argument('--max-run', type=float, default=15)
    ap.add_argument('--no-ddl', action='store_true')
    ap.add_argument('--seed', type=int, default=int(time.time()))
    ap.add_argument('--opt', action='append', default=[], help='extra mysqld option k=v')
    ap.add_argument('--fresh', action='store_true')
    a = ap.parse_args()

    os.makedirs(WORK, exist_ok=True)
    os.makedirs(ART, exist_ok=True)
    opts = dict(o.split('=', 1) for o in a.opt)
    rng = random.Random(a.seed)
    log('seed', a.seed, 'opts', opts)

    if a.fresh or not os.path.exists(DATADIR):
        init_datadir(opts)
    else:
        write_cnf(opts)
    if os.path.exists(ERRLOG):
        os.remove(ERRLOG)
    proc, t = start_mysqld(shim=True)
    if not proc:
        raise SystemExit('initial start failed')
    setup_schema()
    problems, next_seq = verify({}, None)
    log('initial state', 'OK' if not problems else problems)
    errpos = os.path.getsize(ERRLOG)
    modes = a.modes.split(',')
    nbugs = 0

    for it in range(a.iters):
        mode = modes[it % len(modes)]
        wl = Workload(a.clients, not a.no_ddl, rng.random())
        wl.start(next_seq)
        dur = rng.uniform(a.min_run, a.max_run)
        time.sleep(dur)
        # freeze + kill + loss
        seed = rng.randrange(1 << 30)
        stats, npend, totals = pl.crash(proc.pid, JOURNAL, UNDO, mode, seed)
        wl.halt()
        proc.wait()
        for th in wl.threads:
            th.join(timeout=5)
        acks = {k: set(v) for k, v in wl.acks.items()}
        nack = sum(len(v) for v in acks.values())
        log(f'iter {it} mode={mode} ran {dur:.1f}s txns={wl.ntxn} ddl={wl.nddl} acked_rows={nack} pending_writes={npend} loss={totals} wl_errors={len(wl.errors)}')
        if wl.errors:
            log('  workload errors sample:', wl.errors[:3])
        shutil.copy(JOURNAL, JOURNAL + '.last')
        shutil.copy(UNDO, UNDO + '.last')
        import walcheck, dblwrcheck
        nviol = walcheck.main(JOURNAL) + dblwrcheck.main(JOURNAL)
        if nviol:
            log(f'  !!! CHECKER VIOLATIONS: {nviol}')
            d = f'{ART}/{time.strftime("%Y%m%d-%H%M%S")}-viol'
            os.makedirs(d)
            shutil.copy(JOURNAL, d)
            shutil.copy(ERRLOG, d)
            log('  journal ->', d)
        subprocess.run(['tar', 'cf', f'{WORK}/pre.tar', '-C', WORK, 'data'])
        # restart
        proc, t = start_mysqld(shim=True)
        errs, errpos = scan_errlog(errpos)
        info = dict(iter=it, mode=mode, seed=seed, stats=stats, pending=npend, loss=totals,
                    errlog=errs[-40:], acked=nack)
        if not proc:
            log(f'  !!! mysqld failed to restart after {t:.0f}s; errlog tail:')
            for e in errs[-15:]:
                log('   ', e)
            info['journal'] = JOURNAL + '.last'
            save_artifacts(f'nostart-{mode}', info)
            nbugs += 1
            # the datadir is toast: start over
            init_datadir(opts)
            proc, _ = start_mysqld(shim=True)
            setup_schema()
            _, next_seq = verify({}, None)
            errpos = os.path.getsize(ERRLOG)
            continue
        log(f'  restarted in {t:.1f}s, errlog ERRORs: {len(errs)}')
        for e in errs[:5]:
            log('   ', e)
        try:
            problems, next_seq = verify(acks, None)
        except Exception as e:
            problems = [f'verify raised {e!r}']
        if problems:
            nbugs += 1
            log('  !!! VERIFY FAILED:')
            for p in problems:
                log('   ', p)
            info['problems'] = problems
            save_artifacts(f'verify-{mode}', info)
            stop_mysqld(proc)
            init_datadir(opts)
            proc, _ = start_mysqld(shim=True)
            setup_schema()
            _, next_seq = verify({}, None)
            errpos = os.path.getsize(ERRLOG)
        else:
            log('  verify OK')
    log(f'done: {a.iters} iterations, {nbugs} failures')
    stop_mysqld(proc)


if __name__ == '__main__':
    main()
