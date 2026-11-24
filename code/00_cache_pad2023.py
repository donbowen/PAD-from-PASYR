"""
00_cache_pad2023.py -- cache the 2023 PAD release as parquet, for checking the rebuild.

Reads the six zips of the ECORSEXC/2023 release from config.PAD2023_DIR and writes
work/pad2023/<table>.parquet with the release's own column names and types. Nothing in the
rebuild depends on these files except the checks (05_validate.py) and the conveyance-type
lookup (03_conveyance.py), which reuses the 2023 release's classification of texts it has seen.

Usage:  python code/00_cache_pad2023.py [table ...]
"""

from __future__ import annotations

import os
import sys
import shutil
import tempfile
import zipfile

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

import config

CHUNK = 2_000_000


def cache_dta(src: str, dst: str) -> None:
    with tempfile.TemporaryDirectory(dir=config.WORK) as tmp:
        with zipfile.ZipFile(src) as z:
            member = [n for n in z.namelist() if n.endswith('.dta')][0]
            path = z.extract(member, tmp)
        writer = None
        n = 0
        for chunk in pd.read_stata(path, chunksize=CHUNK, convert_categoricals=False):
            for c in chunk.columns:
                if chunk[c].dtype == object:
                    chunk[c] = chunk[c].astype('string')
            t = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                schema = t.schema
                writer = pq.ParquetWriter(dst, schema)
            writer.write_table(t.cast(schema))
            n += len(chunk)
            print(f'    {n:,}', flush=True)
        writer.close()


def cache_csv(src: str, dst: str) -> None:
    with tempfile.TemporaryDirectory(dir=config.WORK) as tmp:
        with zipfile.ZipFile(src) as z:
            member = [n for n in z.namelist() if n.endswith('.csv')][0]
            path = z.extract(member, tmp)
        df = pl.read_csv(path, infer_schema=False, encoding='utf8-lossy')
        df = df.with_columns(pl.col('rf_id').cast(pl.Int64))
        for c in ('exec_dt', 'ack_dt'):
            if c in df.columns:
                df = df.with_columns(pl.col(c).str.to_date('%Y-%m-%d', strict=False))
        df.write_parquet(dst)
        print(f'    {df.height:,}', flush=True)


def main(tables: list[str]) -> None:
    for t in tables:
        src = os.path.join(config.PAD2023_DIR, config.PAD2023_FILES[t])
        dst = os.path.join(config.PAD2023, f'{t}.parquet')
        if os.path.exists(dst):
            print(f'{t}: cached already')
            continue
        print(f'{t}: {src}', flush=True)
        part = dst + '.part'
        (cache_dta if src.endswith('.dta.zip') else cache_csv)(src, part)
        shutil.move(part, dst)


if __name__ == '__main__':
    main(sys.argv[1:] or list(config.PAD2023_FILES))
