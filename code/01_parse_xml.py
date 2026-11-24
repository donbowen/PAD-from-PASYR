"""
01_parse_xml.py -- stream each PASYR zip's XML into flat parquet files, with no cleaning.

For each inputs_raw/<stem>.zip writes work/parsed/<stem>/ with four files:

    record.parquet    one row per <patent-assignment>: reel/frame, dates, correspondent,
                      conveyance text, purge indicator, page count
    assignor.parquet  one row per <patent-assignor>: name, name-type, execution / acknowledged date
    assignee.parquet  one row per <patent-assignee>: name, name-type, address fields
    docid.parquet     one row per <document-id> inside each <patent-property>, with the
                      property's title and language

Every row carries `src` (the zip stem) and `pos` (the record's position in its file), so a
reel/frame that appears more than once can be traced. Values are kept as the XML has them
(text unstripped, dates as YYYYMMDD strings). All interpretation happens in 02_build_tables.py.

Usage:  python code/01_parse_xml.py [--jobs N] [zip stem ...]
"""

from __future__ import annotations

import argparse
import glob
import os
import shutil
import sys
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

import config

FLUSH = 250_000  # records per write

SCHEMAS = {
    'record': ['src', 'pos', 'reel_no', 'frame_no', 'last_update_dt', 'purge_in', 'record_dt',
               'page_count', 'cname', 'caddress_1', 'caddress_2', 'caddress_3', 'caddress_4',
               'convey_text'],
    'assignor': ['src', 'pos', 'reel_no', 'frame_no', 'seq', 'or_name', 'or_name_type',
                 'exec_dt', 'ack_dt'],
    'assignee': ['src', 'pos', 'reel_no', 'frame_no', 'seq', 'ee_name', 'ee_name_type',
                 'ee_address_1', 'ee_address_2', 'ee_city', 'ee_state', 'ee_country',
                 'ee_postcode'],
    'docid': ['src', 'pos', 'reel_no', 'frame_no', 'prop_seq', 'doc_seq', 'country',
              'doc_number', 'kind', 'doc_name', 'date', 'title', 'lang'],
}
INT_COLS = {'pos', 'seq', 'prop_seq', 'doc_seq'}


def _schema(cols):
    return pa.schema([(c, pa.int64() if c in INT_COLS else pa.string()) for c in cols])


def _t(el, path):
    """Text of the first element at `path` under `el`, or None."""
    x = el.find(path)
    return None if x is None else x.text


def _date(el, path):
    x = el.find(path)
    if x is None:
        return None
    d = x.find('date')
    return None if d is None else d.text


def parse_one(zpath: str) -> tuple[str, dict]:
    stem = os.path.splitext(os.path.basename(zpath))[0]
    outdir = os.path.join(config.PARSED, stem)
    done_flag = os.path.join(outdir, '_DONE')
    if os.path.exists(done_flag):
        return stem, {'skipped': True}
    tmpdir = outdir + '.part'
    shutil.rmtree(tmpdir, ignore_errors=True)
    os.makedirs(tmpdir)

    writers = {k: pq.ParquetWriter(os.path.join(tmpdir, f'{k}.parquet'), _schema(v))
               for k, v in SCHEMAS.items()}
    bufs = {k: [] for k in SCHEMAS}
    counts = {k: 0 for k in SCHEMAS}

    def flush():
        for k, rows in bufs.items():
            if rows:
                cols = list(zip(*rows))
                arrs = [pa.array(col, type=pa.int64() if name in INT_COLS else pa.string())
                        for name, col in zip(SCHEMAS[k], cols)]
                writers[k].write_table(pa.Table.from_arrays(arrs, schema=_schema(SCHEMAS[k])))
                counts[k] += len(rows)
                rows.clear()

    t0 = time.time()
    with zipfile.ZipFile(zpath) as z:
        member = [n for n in z.namelist() if n.lower().endswith('.xml')][0]
        with z.open(member) as fh:
            ctx = etree.iterparse(fh, events=('end',), tag='patent-assignment',
                                  load_dtd=False, resolve_entities=False, huge_tree=True,
                                  recover=False)
            pos = 0
            for _, pa_el in ctx:
                rec = pa_el.find('assignment-record')
                reel = _t(rec, 'reel-no')
                frame = _t(rec, 'frame-no')
                corr = rec.find('correspondent')
                if corr is None:
                    cfields = (None,) * 5
                else:
                    cfields = tuple(_t(corr, p) for p in
                                    ('name', 'address-1', 'address-2', 'address-3', 'address-4'))
                bufs['record'].append((
                    stem, pos, reel, frame, _date(rec, 'last-update-date'),
                    _t(rec, 'purge-indicator'), _date(rec, 'recorded-date'),
                    _t(rec, 'page-count'), *cfields, _t(rec, 'conveyance-text')))

                ors = pa_el.find('patent-assignors')
                if ors is not None:
                    for i, a in enumerate(ors.findall('patent-assignor')):
                        nm = a.find('name')
                        bufs['assignor'].append((
                            stem, pos, reel, frame, i,
                            None if nm is None else nm.text,
                            None if nm is None else nm.get('name-type'),
                            _date(a, 'execution-date'), _date(a, 'date-acknowledged')))

                ees = pa_el.find('patent-assignees')
                if ees is not None:
                    for i, a in enumerate(ees.findall('patent-assignee')):
                        nm = a.find('name')
                        bufs['assignee'].append((
                            stem, pos, reel, frame, i,
                            None if nm is None else nm.text,
                            None if nm is None else nm.get('name-type'),
                            _t(a, 'address-1'), _t(a, 'address-2'), _t(a, 'city'),
                            _t(a, 'state'), _t(a, 'country-name'), _t(a, 'postcode')))

                props = pa_el.find('patent-properties')
                if props is not None:
                    for j, p in enumerate(props.findall('patent-property')):
                        ti = p.find('invention-title')
                        title = None if ti is None else ''.join(ti.itertext())
                        lang = None if ti is None else ti.get('lang')
                        dids = p.findall('document-id')
                        if not dids:
                            bufs['docid'].append((stem, pos, reel, frame, j, -1, None, None,
                                                  None, None, None, title, lang))
                        for k, d in enumerate(dids):
                            bufs['docid'].append((
                                stem, pos, reel, frame, j, k, _t(d, 'country'),
                                _t(d, 'doc-number'), _t(d, 'kind'), _t(d, 'name'),
                                _t(d, 'date'), title, lang))

                pa_el.clear()
                while pa_el.getprevious() is not None:
                    del pa_el.getparent()[0]
                pos += 1
                if pos % FLUSH == 0:
                    flush()
            del ctx
    flush()
    for w in writers.values():
        w.close()
    shutil.rmtree(outdir, ignore_errors=True)
    os.rename(tmpdir, outdir)
    counts['seconds'] = round(time.time() - t0)
    with open(done_flag, 'w') as f:
        f.write(repr(counts))
    return stem, counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--jobs', type=int, default=max(1, (os.cpu_count() or 2) - 2))
    ap.add_argument('stems', nargs='*')
    a = ap.parse_args()
    zips = sorted(glob.glob(os.path.join(config.RAW, '*.zip')))
    if a.stems:
        zips = [z for z in zips if os.path.splitext(os.path.basename(z))[0] in a.stems]
    print(f'{len(zips)} zip files, {a.jobs} jobs', flush=True)
    with ProcessPoolExecutor(a.jobs) as ex:
        futs = {ex.submit(parse_one, z): z for z in zips}
        for f in as_completed(futs):
            stem, counts = f.result()
            print(stem, counts, flush=True)


if __name__ == '__main__':
    sys.exit(main())
