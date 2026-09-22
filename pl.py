#!/usr/bin/env python3
"""Power-loss model applied on top of the plshim journal.

crash(pid, log, undo, mode): freeze the process, read the journal, kill the
process, then for every sector that has non-durable writes pick one of:
  keep     - the newest write reached the platter
  old      - roll back to the last durable image (pre-image of the first
             non-durable write) or to an intermediate image
  garbage  - torn sector: random bytes
"""
import os, random, signal, sys, time, struct

SECTOR = 512


def parse_journal(path):
    """Return {file: [(off, len, uoff, ulen), ...]} of non-durable writes, plus stats."""
    pending = {}
    stats = dict(W=0, F=0, T=0, D=0, R=0)
    with open(path, 'r', errors='replace') as f:
        for line in f:
            p = line.rstrip('\n').split(' ')
            t = p[0]
            if t not in stats:
                continue
            stats[t] += 1
            if t == 'W':
                fn, off, ln, uoff, ulen = p[1], int(p[2]), int(p[3]), int(p[4]), int(p[5])
                pending.setdefault(fn, []).append((off, ln, uoff, ulen))
            elif t == 'F':
                pending.pop(p[1], None)
            elif t == 'D':
                pending.pop(p[1], None)
            elif t == 'R':
                # writes to the old name are now under the new one; keep them
                if p[1] in pending:
                    pending[p[2]] = pending.pop(p[1])
    return pending, stats


def apply_loss(pending, undo_path, mode, rng, verbose=True):
    """mode: dict with probabilities p_keep, p_garbage (rest = old/intermediate)."""
    undo = open(undo_path, 'rb') if undo_path and os.path.exists(undo_path) else None
    totals = dict(files=0, sectors=0, keep=0, old=0, garbage=0, zero=0)
    for fn, recs in pending.items():
        if not recs or not os.path.exists(fn):
            continue
        totals['files'] += 1
        # sector -> list of record indices covering it, in journal order
        cover = {}
        for i, (off, ln, uoff, ulen) in enumerate(recs):
            if ln <= 0:
                continue
            for s in range(off // SECTOR, (off + ln - 1) // SECTOR + 1):
                cover.setdefault(s, []).append(i)
        p_garbage = mode['p_garbage']
        if '#innodb_redo/' in fn and not mode.get('redo_garbage', True):
            p_garbage = 0.0
        fd = os.open(fn, os.O_RDWR)
        try:
            size = os.fstat(fd).st_size
            for s, idxs in cover.items():
                totals['sectors'] += 1
                r = rng.random()
                soff = s * SECTOR
                if r < mode['p_keep']:
                    totals['keep'] += 1
                    continue
                if r < mode['p_keep'] + p_garbage:
                    # garble only the bytes the newest write touched in this sector
                    off, ln, _, _ = recs[idxs[-1]]
                    a = max(off, soff)
                    b = min(off + ln, soff + SECTOR)
                    if b > a and a < size:
                        os.pwrite(fd, rng.randbytes(b - a), a)
                    totals['garbage'] += 1
                    continue
                # old / intermediate image: pre-image of record j covering this sector
                j = idxs[0] if (len(idxs) == 1 or rng.random() < 0.5) else rng.choice(idxs)
                off, ln, uoff, ulen = recs[j]
                a = max(off, soff)
                b = min(off + ln, soff + SECTOR)
                if b <= a:
                    continue
                if undo is None or uoff < 0:
                    # nothing to restore from: the file did not exist here -> zeros
                    if a < size:
                        os.pwrite(fd, b'\0' * (b - a), a)
                    totals['zero'] += 1
                    continue
                rel = a - off
                avail = max(0, min(ulen - rel, b - a))
                buf = b''
                if avail > 0:
                    undo.seek(uoff + rel)
                    buf = undo.read(avail)
                buf += b'\0' * ((b - a) - len(buf))
                if a < size:
                    os.pwrite(fd, buf, a)
                totals['old'] += 1
        finally:
            os.close(fd)
    if undo:
        undo.close()
    if verbose:
        print('loss applied:', totals, file=sys.stderr)
    return totals


MODES = {
    'none':    dict(p_keep=1.0, p_garbage=0.0),
    'stale':   dict(p_keep=0.5, p_garbage=0.0),
    'garbage': dict(p_keep=0.5, p_garbage=0.5),
    # torn sectors turn to garbage in data files only; redo sectors are
    # atomic (old or new), which is what InnoDB assumes for the block it
    # rewrites as it fills
    'garbage-data': dict(p_keep=0.5, p_garbage=0.5, redo_garbage=False),
    'mixed':   dict(p_keep=0.4, p_garbage=0.3),
    'allold':  dict(p_keep=0.0, p_garbage=0.0),
}


def crash(pid, log, undo, mode_name, seed=None):
    rng = random.Random(seed)
    os.kill(pid, signal.SIGSTOP)
    time.sleep(0.2)
    pending, stats = parse_journal(log)
    npend = sum(len(v) for v in pending.values())
    print(f'journal: {stats}, pending writes: {npend} in {len(pending)} files', file=sys.stderr)
    os.kill(pid, signal.SIGKILL)
    # wait until it is really gone
    for _ in range(200):
        try:
            os.kill(pid, 0)
            time.sleep(0.05)
        except ProcessLookupError:
            break
    totals = apply_loss(pending, undo, MODES[mode_name], rng)
    return stats, npend, totals


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('pid', type=int)
    ap.add_argument('log')
    ap.add_argument('undo')
    ap.add_argument('--mode', default='mixed', choices=MODES.keys())
    ap.add_argument('--seed', type=int)
    a = ap.parse_args()
    crash(a.pid, a.log, a.undo, a.mode, a.seed)
