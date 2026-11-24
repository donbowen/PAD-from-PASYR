"""
05_documentid_admin.py -- build documentid_admin, with PatentsView standing in for USPTO
administrative data.

The 2023 PAD release checked each (application number, patent number) pair in documentid
against internal USPTO records, which are not public. This script uses PatentsView's
g_application file instead (patent_id <-> application_id, grants from 1976). Patents granted
before 1976 are not in PatentsView, so their admin columns are empty, as they mostly were in
2023. 06_validate.py reports how often the result agrees with the 2023 release.

Two PatentsView quirks are handled:
  * Some 1990s applications carry the wrong two-digit series code (patent 5,878,455 was filed
    in January 1998 as 09/009,793; PatentsView has 08009793). A PatentsView application is
    taken to be the listed one when the 6-digit serial number and the filing date both match.
  * Some application numbers sit on two PatentsView patents. When one of them is the patent
    the transaction lists, that one is used; otherwise the lowest patent number.

One row per distinct (rf_id, appno_doc_num, grant_doc_num) in documentid, as in 2023:

    admin_appl_id_for_grant   application number PatentsView gives for grant_doc_num
    admin_pat_no_for_appno    patent number PatentsView gives for appno_doc_num
    error                     the 2023 release's labels, by its rules (reproduced exactly on
                              99.986% of its rows from its own columns):
        missing appno_doc_num                no application number
        missing grant_doc_num                no patent number, but the application was granted
        incorrect appno_doc_num              the patent belongs to another application, and the
                                             listed application has no patent
        appno and grant mismatch, ambiguous  the two numbers disagree with the admin links
        none                                 otherwise

PatentsView's g_application.tsv.zip is read from the folder above this one (config.PAD2023_DIR)
unless the environment variable PV_G_APPLICATION gives another path.
"""

from __future__ import annotations

import os
import zipfile

import polars as pl

import config

PV = os.environ.get('PV_G_APPLICATION',
                    os.path.join(config.PAD2023_DIR, 'g_application.tsv.zip'))


def pad_patent(col: pl.Expr) -> pl.Expr:
    """PatentsView patent_id -> PAD style: letter prefix + digits zero-padded to 7 characters
    (PP4908 -> PP04908, H368 -> H000368); 7- and 8-digit utility numbers are unchanged."""
    pre = col.str.extract(r'^([A-Z]*)', 1)
    num = col.str.extract(r'(\d+)$', 1)
    width = (pl.lit(7) - pre.str.len_chars()).clip(lower_bound=0)
    padded = pl.concat_str([pre, num.str.zfill(width)])
    return pl.when(num.str.len_chars() + pre.str.len_chars() >= 7).then(col).otherwise(padded)


def load_pv() -> pl.DataFrame:
    cache = config.work('pv_g_application.parquet')
    cols = ['application_id', 'patent_id', 'filing_date']
    if not os.path.exists(cache) or pl.read_parquet_schema(cache).keys() != set(cols):
        with zipfile.ZipFile(PV) as z:
            name = [n for n in z.namelist() if n.endswith('.tsv')][0]
            raw = z.open(name).read()
        pl.read_csv(raw, separator='\t', infer_schema=False, quote_char='"',
                    columns=cols).write_parquet(cache)
    pv = pl.read_parquet(cache)
    app = pl.col('application_id')
    return pv.select(
        pl.when(app.str.starts_with('D')).then(pl.concat_str([pl.lit('29'), app.str.slice(1)]))
          .otherwise(app).alias('pv_app'),
        pad_patent(pl.col('patent_id')).alias('pv_pat'),
        pl.col('filing_date').str.to_date(strict=False).alias('pv_fd'))


def main():
    pv = load_pv()
    for universe in config.UNIVERSES:
        build(pv, universe)


def build(pv: pl.DataFrame, universe: str):
    doc = (pl.read_parquet(config.table(universe, 'documentid'),
                           columns=['rf_id', 'appno_doc_num', 'grant_doc_num', 'appno_date'])
             .unique(['rf_id', 'appno_doc_num', 'grant_doc_num'], keep='first', maintain_order=True)
             .with_row_index('_r'))
    serial = pl.col('appno_doc_num').str.slice(2)

    # admin_appl_id_for_grant: PatentsView's application for the listed patent
    g = (doc.select('_r', 'appno_doc_num', 'appno_date', 'grant_doc_num')
            .join(pv, left_on='grant_doc_num', right_on='pv_pat', how='inner')
            .with_columns(pl.when((pl.col('pv_app') != pl.col('appno_doc_num'))
                                  & (pl.col('pv_app').str.slice(2) == serial)
                                  & (pl.col('pv_fd') == pl.col('appno_date')))
                            .then(pl.col('appno_doc_num')).otherwise(pl.col('pv_app'))
                            .alias('admin_appl_id_for_grant'))
            .group_by('_r').agg(pl.col('admin_appl_id_for_grant').first()))

    # admin_pat_no_for_appno: PatentsView's patent for the listed application, found by number
    # or by (serial, filing date); prefer the listed patent if it is among the candidates
    base = doc.filter(pl.col('appno_doc_num').is_not_null()).select(
        '_r', 'appno_doc_num', 'appno_date', 'grant_doc_num')
    by_num = base.join(pv.select('pv_app', 'pv_pat'), left_on='appno_doc_num',
                       right_on='pv_app', how='inner').select('_r', 'grant_doc_num', 'pv_pat')
    by_ser = (base.with_columns(serial.alias('_s'))
                  .join(pv.with_columns(pl.col('pv_app').str.slice(2).alias('_s')),
                        left_on=['_s', 'appno_date'], right_on=['_s', 'pv_fd'], how='inner')
                  .select('_r', 'grant_doc_num', 'pv_pat'))
    a = (pl.concat([by_num, by_ser]).unique()
           .with_columns((pl.col('pv_pat') == pl.col('grant_doc_num')).fill_null(False).alias('_hit'))
           .sort(['_r', '_hit', 'pv_pat'], descending=[False, True, False])
           .group_by('_r', maintain_order=True)
           .agg(pl.col('pv_pat').first().alias('admin_pat_no_for_appno')))

    d = doc.join(g, on='_r', how='left').join(a, on='_r', how='left')
    A, G, AA, AP = (pl.col(c) for c in ['appno_doc_num', 'grant_doc_num',
                                        'admin_appl_id_for_grant', 'admin_pat_no_for_appno'])
    err = (pl.when(A.is_null()).then(pl.lit('missing appno_doc_num'))
             .when(G.is_null() & AP.is_not_null()).then(pl.lit('missing grant_doc_num'))
             .when(G.is_not_null() & AA.is_not_null() & (AA != A) & AP.is_null())
               .then(pl.lit('incorrect appno_doc_num'))
             .when(G.is_not_null() & ((AA.is_not_null() & (AA != A)) | (AP.is_not_null() & (AP != G))))
               .then(pl.lit('appno and grant mismatch, ambiguous'))
             .otherwise(pl.lit('none')))
    out = (d.sort('_r').with_columns(err.alias('error'))
            .select('rf_id', 'appno_doc_num', 'grant_doc_num', 'admin_appl_id_for_grant',
                    'admin_pat_no_for_appno', 'error'))
    out.write_parquet(config.table(universe, 'documentid_admin'))
    print(f'{universe}: documentid_admin {out.height:,} rows')
    with pl.Config(tbl_rows=10, fmt_str_lengths=40):
        print(out['error'].value_counts().sort('count', descending=True))


if __name__ == '__main__':
    main()
