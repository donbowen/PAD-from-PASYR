"""
03_universe.py -- choose which transactions and properties go in the tables.

The PASYR XML holds every recorded transaction, including those on applications that were never
published: provisional applications (series 60-63), PCT applications, and non-provisional
applications abandoned before publication. The 2023 PAD release left those out. Its documentid
holds only properties with a pre-grant publication number or a patent number, plus Hague
international design applications (series 35); 1,271 of its 18.8 million rows break that rule.
Its transactions are those with at least one such property: for record years before 2018 it has
99.98% or more of them, and none of the transactions on unpublished applications only.

So two versions are written:

  outputs/       ("published", the drop-in) properties that are published, granted, or Hague
                 designs, and the transactions with at least one of them. assignor and assignee
                 rows follow their transaction.
  outputs/all/   ("all") everything in work/full/, unchanged.

A property counts as published if it has a publication or patent number in the 2026 XML, so
applications published after January 2024 are now in, as an update should have them.
"""

from __future__ import annotations

import os
import shutil

import polars as pl

import config

TABLES = ['assignment', 'assignor', 'assignee', 'documentid']


def main():
    src = {t: os.path.join(config.FULL, f'{t}.parquet') for t in TABLES}
    for t in TABLES:
        shutil.copyfile(src[t], config.table('all', t))

    doc = pl.read_parquet(src['documentid'])
    public = (pl.col('pgpub_doc_num').is_not_null() | pl.col('grant_doc_num').is_not_null()
              | pl.col('appno_doc_num').str.starts_with('35'))
    pub = doc.filter(public)
    keep = pub.select('rf_id').unique()
    pub.write_parquet(config.table('published', 'documentid'))
    for t in ['assignment', 'assignor', 'assignee']:
        df = pl.read_parquet(src[t]).join(keep, on='rf_id', how='semi')
        df.write_parquet(config.table('published', t))
    n_all = pl.read_parquet(src['assignment'], columns=['rf_id']).height
    print(f'transactions: {n_all:,} in all, {keep.height:,} published '
          f'({n_all - keep.height:,} only on unpublished applications)')
    print(f'properties:   {doc.height:,} in all, {pub.height:,} published')


if __name__ == '__main__':
    main()
