#!/usr/bin/env python3
"""WAL-rule checker for plshim journals.

Every data page written to a redo-logged file must have FIL_PAGE_LSN <= the
redo LSN that was durable (written and fsynced) at the moment the page write
was issued.  The journal is written under a mutex before the page write and
after the redo fsync, so the order of lines is causal.
"""
import sys, collections

SKIP = ('#innodb_temp/', 'ibtmp', '.dblwr')   # dblwr checked separately below

def main(path, verbose=False):
    redo_written = collections.defaultdict(int)   # redo file -> max lsn written
    redo_durable = 0
    armed = False   # redo written before this journal started is durable but unknown; check only after the first redo fsync
    n_pages = 0
    viol = []
    maxpage = 0
    with open(path, errors='replace') as f:
        for ln, line in enumerate(f, 1):
            p = line.rstrip('\n').split(' ')
            t = p[0]
            if t == 'W' and len(p) >= 7:
                fn, off, sz, lsn = p[1], int(p[2]), int(p[3]), int(p[6])
                if '#innodb_redo/' in fn:
                    if lsn > redo_written[fn]:
                        redo_written[fn] = lsn
                elif lsn and not any(k in fn for k in SKIP[:2]):
                    n_pages += 1
                    maxpage = max(maxpage, lsn)
                    if armed and lsn > redo_durable:
                        viol.append((ln, fn, off, sz, lsn, redo_durable))
            elif t == 'F' and '#innodb_redo/' in p[1]:
                redo_durable = max(redo_durable, redo_written.get(p[1], 0))
                if redo_durable:
                    armed = True
            elif t == 'R' and '#innodb_redo/' in p[1]:
                redo_written[p[2]] = max(redo_written.get(p[2], 0), redo_written.pop(p[1], 0))
    print(f'{path}: page writes checked={n_pages} max page lsn={maxpage} redo durable at end={redo_durable} violations={len(viol)}')
    for v in viol[:20 if not verbose else None]:
        ln, fn, off, sz, lsn, dur = v
        print(f'  line {ln}: {fn.split("/data/")[-1]} off={off} len={sz} page_lsn={lsn} > redo_durable={dur} (+{lsn-dur})')
    return len(viol)

if __name__ == '__main__':
    rc = 0
    for a in sys.argv[1:]:
        rc |= bool(main(a))
    sys.exit(rc)
