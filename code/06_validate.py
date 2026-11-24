"""
06_validate.py -- compare the rebuilt tables with the 2023 PAD release, column by column.

For every table, on the rf_ids that are in both:
  * one-row-per-rf_id tables (assignment, assignment_conveyance): the share of rf_ids where
    each column agrees, and how the rest disagree: filled (missing in 2023, present now --
    usually an update), emptied (present in 2023, missing now), changed (both present);
  * many-rows-per-rf_id tables (assignor, assignee, documentid, documentid_admin): the share of
    rf_ids where the number of rows agrees, and where the sorted list of each column's values
    agrees (so row order does not matter). documentid and documentid_admin are also matched
    row by row, on (rf_id, appno_doc_num) and (rf_id, appno_doc_num, grant_doc_num), with the
    same filled / emptied / changed split (<table>_rows.csv).
Text is compared after stripping white space, with empty text treated as missing. Also reports
rf_id coverage by record year: in both, only in the rebuild, only in 2023.

Compares the published version (outputs/, the drop-in) by default; pass --all for outputs/all/.
Writes outputs/validation/<table>.csv and prints a summary. Run 00_cache_pad2023.py first.

Usage:  python code/05_validate.py [table ...]
"""

from __future__ import annotations

import os
import sys

import polars as pl

import config

ONE_ROW = {'assignment', 'assignment_conveyance'}
UNIVERSE = 'published'
SKIP = {'rf_id'}


def norm(df: pl.DataFrame) -> pl.DataFrame:
    ex = []
    for c, t in df.schema.items():
        if c == 'rf_id':
            ex.append(pl.col(c).cast(pl.Int64))
        elif t == pl.String:
            s = pl.col(c).str.strip_chars()
            ex.append(pl.when(s == '').then(None).otherwise(s).alias(c))
        elif isinstance(t, pl.Datetime):
            ex.append(pl.col(c).dt.date())
        elif t.is_numeric():
            ex.append(pl.col(c).cast(pl.Float64))
        else:
            ex.append(pl.col(c))
    return df.select(ex)


def load(table: str):
    new = pl.read_parquet(config.table(UNIVERSE, table))
    old = pl.read_parquet(os.path.join(config.PAD2023, f'{table}.parquet'))
    cols = [c for c in old.columns if c in new.columns]
    return norm(new.select(cols)), norm(old.select(cols)), cols


def compare_keyed(new, old, keys, cols):
    """Join on `keys` and sort each column's disagreements into: filled (2023 missing, now
    present: usually an update), emptied (2023 present, now missing), changed (both present)."""
    new = new.unique(keys, keep='first')
    old = old.unique(keys, keep='first')
    j = new.join(old, on=keys, how='inner', suffix='_old', nulls_equal=True)
    rows = [{'column': '(rows matched on ' + '+'.join(keys) + '; filled = rows only in new, '
                       'emptied = rows only in 2023)', 'n': j.height,
             'share_equal': None, 'filled': new.height - j.height, 'emptied': old.height - j.height,
             'changed': None}]
    for c in cols:
        if c in SKIP or c in keys:
            continue
        a, b = pl.col(c), pl.col(f'{c}_old')
        rows.append({'column': c, 'n': j.height,
                     'share_equal': j.select(a.eq_missing(b).mean()).item(),
                     'filled': j.select((a.is_not_null() & b.is_null()).sum()).item(),
                     'emptied': j.select((a.is_null() & b.is_not_null()).sum()).item(),
                     'changed': j.select((a.is_not_null() & b.is_not_null() & (a != b)).sum()).item()})
    return pl.DataFrame(rows, schema_overrides={'share_equal': pl.Float64, 'changed': pl.Int64})


# For tables with several rows per rf_id, rows are also matched on these keys.
ROW_KEYS = {'documentid': ['rf_id', 'appno_doc_num'],
            'documentid_admin': ['rf_id', 'appno_doc_num', 'grant_doc_num']}


def compare_many_rows(new, old, cols):
    both = new.select('rf_id').unique().join(old.select('rf_id').unique(), on='rf_id')
    new = new.join(both, on='rf_id', how='semi')
    old = old.join(both, on='rf_id', how='semi')
    cnt = (new.group_by('rf_id').len().join(old.group_by('rf_id').len(), on='rf_id', suffix='_old'))
    rows = [{'column': '(row count)', 'n_rf_id': both.height,
             'share_equal': cnt.select((pl.col('len') == pl.col('len_old')).mean()).item(),
             'n_differ': cnt.select((pl.col('len') != pl.col('len_old')).sum()).item()}]
    for c in cols:
        if c in SKIP:
            continue
        v = pl.col(c).cast(pl.String).fill_null('<NA>').sort()
        a = new.group_by('rf_id').agg(v.alias('n'))
        b = old.group_by('rf_id').agg(v.alias('o'))
        ab = a.join(b, on='rf_id')
        eq = ab.select((pl.col('n') == pl.col('o')))
        rows.append({'column': c, 'n_rf_id': ab.height, 'share_equal': eq.to_series().mean(),
                     'n_differ': int((~eq.to_series()).sum())})
    return pl.DataFrame(rows)


def coverage():
    new = pl.read_parquet(config.table(UNIVERSE, 'assignment'), columns=['rf_id', 'record_dt'])
    old = norm(pl.read_parquet(os.path.join(config.PAD2023, 'assignment.parquet'),
                               columns=['rf_id', 'record_dt']))
    j = new.join(old, on='rf_id', how='full', coalesce=True, suffix='_old')
    j = j.with_columns(pl.coalesce('record_dt_old', 'record_dt').dt.year().alias('record_year'),
                       pl.when(pl.col('record_dt').is_null()).then(pl.lit('only_2023'))
                         .when(pl.col('record_dt_old').is_null()).then(pl.lit('only_new'))
                         .otherwise(pl.lit('both')).alias('where'))
    return (j.group_by('record_year', 'where').len()
             .pivot(on='where', index='record_year', values='len').fill_null(0)
             .sort('record_year'))


def main(tables):
    vdir = os.path.join(config.OUT, 'validation' if UNIVERSE == 'published' else 'validation_all')
    os.makedirs(vdir, exist_ok=True)
    cov = coverage()
    cov.write_csv(os.path.join(vdir, 'coverage_by_year.csv'))
    tot = cov.select(pl.exclude('record_year').sum())
    print('rf_id coverage:', tot.row(0, named=True))
    for t in tables:
        if not os.path.exists(config.table(UNIVERSE, t)):
            print(f'{t}: not built yet')
            continue
        new, old, cols = load(t)
        if t in ONE_ROW:
            res = compare_keyed(new, old, ['rf_id'], cols)
        else:
            res = compare_many_rows(new, old, cols)
        res.write_csv(os.path.join(vdir, f'{t}.csv'))
        with pl.Config(tbl_rows=40, tbl_width_chars=140):
            print(f'\n== {t}')
            print(res)
        if t in ROW_KEYS:
            both = new.select('rf_id').unique().join(old.select('rf_id').unique(), on='rf_id')
            rk = compare_keyed(new.join(both, on='rf_id', how='semi'),
                               old.join(both, on='rf_id', how='semi'), ROW_KEYS[t], cols)
            rk.write_csv(os.path.join(vdir, f'{t}_rows.csv'))
            with pl.Config(tbl_rows=40, tbl_width_chars=140):
                print(f'\n== {t}, row by row (rf_ids in both)')
                print(rk)


if __name__ == '__main__':
    args = sys.argv[1:]
    if '--all' in args:
        UNIVERSE = 'all'
        args.remove('--all')
    main(args or list(config.PAD2023_FILES))
