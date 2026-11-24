"""
02_build_tables.py -- turn the parsed XML into the four PAD tables that come straight from it.

Reads work/parsed/*/ (from 01_parse_xml.py) and writes, in the layout of the 2023 PAD release,
every transaction and property in the XML (03_universe.py then picks the drop-in subset):

    work/full/assignment.parquet   one row per rf_id
    work/full/assignor.parquet     one row per assignor of each rf_id
    work/full/assignee.parquet     one row per assignee of each rf_id
    work/full/documentid.parquet   one row per property (patent / application) of each rf_id

rf_id = reel_no * 10000 + frame_no, as in the release. When a reel/frame appears more than
once in the XML, the copy with the latest last-update-date is kept (ties: the later file, then
the later position in the file), as the 2015 working paper describes (its footnote 36).

Text is kept as the XML has it, except that runs of white space become one space, leading and
trailing white space is removed, and empty strings become missing -- as in the 2023 release
("HACKENSACK, NJ  07601" there is "HACKENSACK, NJ 07601"). Dates become dates; a date that is
not a valid calendar date (such as 00000000) becomes missing.

Repeated reel/frames: in the April 2026 backfile all 315,572 are identical copies where
consecutive parts overlap (the last records of one part open the next).

The XML's name-type attribute ("natural" / "legal") is parsed but not kept: it is empty in
every record of the 2026 backfile.
"""

from __future__ import annotations

import glob
import os

import polars as pl

import config


def scan(table: str) -> pl.LazyFrame:
    files = sorted(glob.glob(os.path.join(config.PARSED, '*', f'{table}.parquet')))
    if not files:
        raise SystemExit(f'no parsed {table} files; run 01_parse_xml.py first')
    return pl.scan_parquet(files)


def clean(col: str) -> pl.Expr:
    c = pl.col(col).str.replace_all(r'\s+', ' ').str.strip_chars()
    return pl.when(c == '').then(None).otherwise(c).alias(col)


def to_date(col: str) -> pl.Expr:
    return pl.col(col).str.strip_chars().str.to_date('%Y%m%d', strict=False).alias(col)


def file_id(src: pl.Expr) -> pl.Expr:
    """The part number of the source file, e.g. ad19000815-20260409-07 -> 7."""
    return src.str.extract(r'-(\d+)$').cast(pl.Int16)


RFID = (pl.col('reel_no').cast(pl.Int64) * 10000 + pl.col('frame_no').cast(pl.Int64)).alias('rf_id')


def keep_copies() -> pl.DataFrame:
    """(src, pos) of the one XML copy kept for each rf_id, plus a count of copies."""
    rec = (scan('record')
           .select('src', 'pos', RFID, to_date('last_update_dt'))
           .collect())
    rec = rec.with_columns(file_id(pl.col('src')).alias('_f'))
    kept = (rec.sort(['rf_id', 'last_update_dt', '_f', 'pos'], nulls_last=False)
               .group_by('rf_id', maintain_order=True)
               .agg(pl.col('src').last(), pl.col('pos').last(), pl.len().alias('n_copies')))
    return kept


def build_assignment(kept: pl.DataFrame) -> pl.DataFrame:
    rec = scan('record').join(kept.lazy().select('src', 'pos'), on=['src', 'pos'], how='semi')
    out = (rec.select(
                RFID,
                file_id(pl.col('src')).alias('file_id'),
                clean('cname'), clean('caddress_1'), clean('caddress_2'), clean('caddress_3'),
                clean('caddress_4'),
                pl.col('reel_no').cast(pl.Int32), pl.col('frame_no').cast(pl.Int32),
                clean('convey_text'),
                to_date('record_dt'), to_date('last_update_dt'),
                pl.col('page_count').str.strip_chars().cast(pl.Int32, strict=False),
                # 1 if purged, else missing, as in the 2023 release (every record is "N")
                pl.when(pl.col('purge_in').str.strip_chars() == 'Y').then(pl.lit(1, pl.Int8))
                  .otherwise(pl.lit(None, pl.Int8)).alias('purge_in'))
              .sort('rf_id')
              .collect())
    return out


def build_assignor(kept: pl.DataFrame) -> pl.DataFrame:
    t = scan('assignor').join(kept.lazy().select('src', 'pos'), on=['src', 'pos'], how='semi')
    return (t.select(RFID, 'seq', clean('or_name'), to_date('exec_dt'), to_date('ack_dt'))
             .sort(['rf_id', 'seq']).drop('seq').collect())


def build_assignee(kept: pl.DataFrame) -> pl.DataFrame:
    t = scan('assignee').join(kept.lazy().select('src', 'pos'), on=['src', 'pos'], how='semi')
    return (t.select(RFID, 'seq', clean('ee_name'), clean('ee_address_1'), clean('ee_address_2'),
                     clean('ee_city'), clean('ee_state'), clean('ee_postcode'),
                     clean('ee_country'))
             .sort(['rf_id', 'seq']).drop('seq').collect())


def build_documentid(kept: pl.DataFrame) -> pl.DataFrame:
    """One row per <patent-property>. Its <document-id>s are told apart by kind code:

        X0       application number (8 digits, or PCT/CCYYYY/NNNNNN)  -> appno_*
        A1 (A*)  pre-grant publication number (11 digits)            -> pgpub_*
        B1, B2   patent number (7-8 characters, e.g. 1234567, D512345) -> grant_*

    In the PASYR files these are the only kind codes. (They are not the official kind codes:
    B1/B2 here only tells whether a publication came first.) A document-id with another kind
    is assigned by position, as the file description sets out: first = application, then
    patent, then publication.
    """
    t = scan('docid').join(kept.lazy().select('src', 'pos'), on=['src', 'pos'], how='semi')
    kind = pl.col('kind').str.strip_chars()
    num = pl.col('doc_number').str.strip_chars()
    role = (pl.when(kind == 'X0').then(pl.lit('appno'))
              .when(kind.str.starts_with('A')).then(pl.lit('pgpub'))
              .when(kind.str.starts_with('B')).then(pl.lit('grant'))
              .when(pl.col('doc_seq') == 0).then(pl.lit('appno'))
              .when(num.str.contains(r'^\d{11}$')).then(pl.lit('pgpub'))
              .otherwise(pl.lit('grant')))
    t = t.with_columns(RFID, role.alias('role'), num.alias('doc_number'),
                       to_date('date'), pl.col('country').str.strip_chars())
    keys = ['src', 'pos', 'rf_id', 'prop_seq']
    t = t.with_columns(clean('title'), clean('lang'))  # before the group-by: much faster
    props = (t.group_by(keys).agg(pl.col('title').first(), pl.col('lang').first()))
    wide = props
    for r in ('appno', 'pgpub', 'grant'):
        part = (t.filter(pl.col('role') == r)
                 .group_by(keys)
                 .agg(pl.col('doc_number').first().alias(f'{r}_doc_num'),
                      pl.col('date').first().alias(f'{r}_date'),
                      pl.col('country').first().alias(f'{r}_country'),
                      pl.len().alias(f'_n_{r}')))
        wide = wide.join(part, on=keys, how='left')
    cols = ['rf_id', 'title', 'lang', 'appno_doc_num', 'appno_date', 'appno_country',
            'pgpub_doc_num', 'pgpub_date', 'pgpub_country', 'grant_doc_num', 'grant_date',
            'grant_country']
    df = wide.sort(['rf_id', 'prop_seq']).collect()
    extra = df.select([pl.col(f'_n_{r}').fill_null(0).gt(1).sum().alias(r)
                       for r in ('appno', 'pgpub', 'grant')])
    print('  properties with more than one document-id of a role:', extra.row(0, named=True))
    return df.select(cols)


if __name__ == '__main__':
    import sys
    kept = keep_copies()
    print(f'rf_ids: {kept.height:,}; with more than one XML copy: '
          f'{kept.filter(pl.col("n_copies") > 1).height:,}', flush=True)
    kept.write_parquet(config.work('kept_copies.parquet'))
    for name, fn in [('assignment', build_assignment), ('assignor', build_assignor),
                     ('assignee', build_assignee), ('documentid', build_documentid)]:
        df = fn(kept)
        df.write_parquet(config.work('full', f'{name}.parquet'))
        print(f'{name}: {df.height:,} rows', flush=True)
