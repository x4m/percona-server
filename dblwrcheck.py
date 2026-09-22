#!/usr/bin/env python3
"""Doublewrite-rule checker for plshim journals (P lines).

Rules checked, in journal (causal) order:
 A. A data page write (space, page, lsn) must be preceded by a write of the
    same (space, page, lsn) into a .dblwr slot that has already been fsynced.
    Pages with lsn == 0 (zero-fill / file extension) are ignored.
 B. A .dblwr slot must not be overwritten while some data page it protects
    has been written but not yet fsynced (its file had no F since) and the
    file still exists.  A hit whose file is unlinked later in the same run is
    reported separately as benign: the space was being dropped or truncated
    (DROP TABLE, undo truncate), which is logged and redone after a crash, so
    its remaining content is dead; fil_flush_file_spaces() skips such spaces
    deliberately (stop_new_ops).
Data pages written before the first redo flush (crash recovery restoring
pages from the doublewrite buffer) have no dblwr copy by design and are
reported as no-dblwr-copy, not as violations.
"""
import sys, collections

def is_data(fn):
    return ('#innodb_redo/' not in fn and '.dblwr' not in fn
            and '#innodb_temp/' not in fn and 'ibtmp' not in fn)

def main(path, limit=15):
    slot = {}                       # (dblwr_file, off) -> [space, page, lsn, durable]
    by_key = collections.defaultdict(set)   # (space,page,lsn) -> set of slot ids
    pend_data = collections.defaultdict(list)  # data file -> [(space,page,lsn)]
    need = collections.Counter()    # (space,page,lsn) still needing protection
    key_file = {}                   # (space,page,lsn) -> data file it was written to
    deleted = {}                    # data file -> line where it was unlinked
    viol_a, viol_b, nodblwr = [], [], []
    n_data = n_dblwr = 0
    with open(path, errors='replace') as f:
        for ln, line in enumerate(f, 1):
            p = line.rstrip('\n').split(' ')
            t = p[0]
            if t == 'P':
                fn, off, space, page, lsn = p[1], int(p[2]), int(p[3]), int(p[4]), int(p[5])
                key = (space, page, lsn)
                if '.dblwr' in fn:
                    n_dblwr += 1
                    sid = (fn, off)
                    old = slot.get(sid)
                    if old is not None:
                        okey = tuple(old[:3])
                        if need[okey] > 0:
                            viol_b.append((ln, sid, okey, key_file.get(okey)))
                        by_key[okey].discard(sid)
                    slot[sid] = [space, page, lsn, False]
                    by_key[key].add(sid)
                elif is_data(fn) and lsn != 0:
                    n_data += 1
                    protected = [s for s in by_key.get(key, ()) if slot[s][3]]
                    if not protected:
                        if by_key.get(key):
                            viol_a.append((ln, fn, key, 'dblwr copy not fsynced yet'))
                        else:
                            nodblwr.append((ln, fn, key))
                    pend_data[fn].append(key)
                    need[key] += 1
                    key_file[key] = fn
            elif t == 'F':
                fn = p[1]
                if '.dblwr' in fn:
                    for sid, v in slot.items():
                        if sid[0] == fn:
                            v[3] = True
                elif fn in pend_data:
                    for key in pend_data.pop(fn):
                        need[key] -= 1
            elif t == 'R':
                if p[1] in pend_data:
                    pend_data[p[2]] = pend_data.pop(p[1])
            elif t == 'D' or (t == 'T' and len(p) > 2 and p[2] == '0'):
                deleted[p[1]] = ln
                if p[1] in pend_data:
                    for key in pend_data.pop(p[1]):
                        need[key] -= 1
    benign = lambda v: v[3] in deleted and deleted[v[3]] > v[0]
    benign_b = [v for v in viol_b if benign(v)]
    viol_b = [v for v in viol_b if not benign(v)]
    short = lambda fn: fn.split('/data/')[-1]
    print(f'{path}: data pages={n_data} dblwr pages={n_dblwr} A(written before dblwr copy durable)={len(viol_a)} '
          f'B(slot reused while protecting pending page)={len(viol_b)} '
          f'B-benign(file unlinked later)={len(benign_b)} no-dblwr-copy={len(nodblwr)}')
    for ln, fn, key, why in viol_a[:limit]:
        print(f'  A line {ln}: {short(fn)} space={key[0]} page={key[1]} lsn={key[2]}: {why}')
    for ln, sid, key, fn in viol_b[:limit]:
        print(f'  B line {ln}: slot {short(sid[0])}@{sid[1]} overwritten; protected {short(fn or "?")} '
              f'space={key[0]} page={key[1]} lsn={key[2]}')
    byfile = collections.Counter(short(fn) for _, fn, _ in nodblwr)
    if byfile:
        print('  no-dblwr-copy by file:', dict(byfile.most_common(10)))
    return len(viol_a) + len(viol_b)

if __name__ == '__main__':
    rc = 0
    for a in sys.argv[1:]:
        rc |= bool(main(a))
    sys.exit(rc)
