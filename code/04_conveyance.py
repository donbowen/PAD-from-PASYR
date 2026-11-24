"""
04_conveyance.py -- build assignment_conveyance (rf_id, convey_ty, employer_assign).

convey_ty. The 2023 PAD release's conveyance type is a fixed function of the conveyance text:
no text in it has two types. The USPTO has since reworded many texts (e.g. "ASSIGNMENT OF
ASSIGNORS INTEREST (SEE DOCUMENT FOR DETAILS)." is now "ASSIGNMENT OF ASSIGNOR'S INTEREST"), so
each text gets its type from the first of:

  1. the type the 2023 release gave the same rf_ids, for a text seen on rf_ids that are in both
     (the most common type, if the reworded text covers several old texts);
  2. the type the 2023 release gave the identical text;
  3. the keyword rules in conveyance_rules.py.

Steps 1 and 2 are saved in data/convey_ty_lookup.csv.gz, so the 2023 release is not needed to
rerun this script: if work/pad2023/ is absent the saved lookup is used.

employer_assign. Reverse-engineered from the 2023 release (it reproduces 99.92% of its flags;
the 2015 working paper, section 4.2.2, describes the same idea). A transaction is an employer
assignment if its type is "employee", or if all of these hold:
  a) its type is "assignment";
  b) it conveys one property (one distinct application / patent number pair);
  c) it is that property's earliest transaction: no transaction on the same application number
     has an earlier execution date (a transaction's execution date is its assignors' latest);
  d) it was executed on or before the patent's grant date (or the property has no grant).

Writes outputs/[all/]assignment_conveyance.parquet, for both versions of the tables (see
03_universe.py), and work/convey_ty_source.parquet (which step typed each rf_id). employer_assign
is computed within each version, since the published version has fewer properties per
transaction (condition b) and fewer transactions per property (condition c).
"""

from __future__ import annotations

import os

import polars as pl

import config
from conveyance_rules import classify

LOOKUP = os.path.join(config.ROOT, 'data', 'convey_ty_lookup.csv.gz')


def strip(col: str) -> pl.Expr:
    s = pl.col(col).str.strip_chars()
    return pl.when(s == '').then(None).otherwise(s).alias(col)


def build_lookup(new: pl.DataFrame) -> pl.DataFrame:
    """text -> convey_ty from the 2023 release; columns convey_text, convey_ty, step, n_rf_id."""
    old_a = pl.read_parquet(os.path.join(config.PAD2023, 'assignment.parquet'),
                            columns=['rf_id', 'convey_text'])
    old_c = pl.read_parquet(os.path.join(config.PAD2023, 'assignment_conveyance.parquet'),
                            columns=['rf_id', 'convey_ty'])
    old = (old_a.join(old_c, on='rf_id')
                .with_columns(pl.col('rf_id').cast(pl.Int64), strip('convey_text')))
    # 1. texts on rf_ids in both releases
    s1 = (new.select('rf_id', 'convey_text').join(old.select('rf_id', 'convey_ty'), on='rf_id')
             .filter(pl.col('convey_text').is_not_null())
             .group_by('convey_text', 'convey_ty').len()
             .sort(['convey_text', 'len', 'convey_ty'], descending=[False, True, False])
             .group_by('convey_text', maintain_order=True)
             .agg(pl.col('convey_ty').first(), pl.col('len').sum().alias('n_rf_id'))
             .with_columns(pl.lit(1).alias('step')))
    # 2. texts of the 2023 release itself
    s2 = (old.filter(pl.col('convey_text').is_not_null())
             .group_by('convey_text').agg(pl.col('convey_ty').first(), pl.len().alias('n_rf_id'))
             .join(s1.select('convey_text'), on='convey_text', how='anti')
             .with_columns(pl.lit(2).alias('step')))
    lk = pl.concat([s1, s2]).select('convey_text', 'convey_ty', 'step', 'n_rf_id').sort('convey_text')
    os.makedirs(os.path.dirname(LOOKUP), exist_ok=True)
    lk.write_csv(LOOKUP + '.tmp')
    import gzip, shutil
    with open(LOOKUP + '.tmp', 'rb') as fi, gzip.open(LOOKUP, 'wb', compresslevel=9) as fo:
        shutil.copyfileobj(fi, fo)
    os.remove(LOOKUP + '.tmp')
    return lk


def load_lookup(new: pl.DataFrame) -> pl.DataFrame:
    if os.path.exists(os.path.join(config.PAD2023, 'assignment_conveyance.parquet')):
        print('building the conveyance-text lookup from the 2023 release')
        return build_lookup(new)
    print(f'2023 release not cached; using the saved lookup {LOOKUP}')
    return pl.read_csv(LOOKUP, schema_overrides={'convey_text': pl.String, 'convey_ty': pl.String})


def convey_types(new: pl.DataFrame) -> pl.DataFrame:
    lk = load_lookup(new).select('convey_text', 'convey_ty', 'step')
    out = (new.select('rf_id', 'convey_text')
              .join(lk, on='convey_text', how='left')
              .with_columns(pl.when(pl.col('convey_ty').is_null()).then(classify(pl.col('convey_text')))
                              .otherwise(pl.col('convey_ty')).alias('convey_ty'),
                            pl.col('step').fill_null(3)))
    return out


def employer_assign(ct: pl.DataFrame, universe: str) -> pl.DataFrame:
    doc = pl.read_parquet(config.table(universe, 'documentid'),
                          columns=['rf_id', 'appno_doc_num', 'grant_doc_num', 'grant_date'])
    ors = pl.read_parquet(config.table(universe, 'assignor'), columns=['rf_id', 'exec_dt'])
    ex = ors.group_by('rf_id').agg(pl.col('exec_dt').max().alias('ex'))
    nprop = doc.group_by('rf_id').agg(pl.struct('appno_doc_num', 'grant_doc_num').n_unique().alias('nprop'))
    d = (doc.filter(pl.col('appno_doc_num').is_not_null())
            .join(ex, on='rf_id', how='left')
            .with_columns(pl.col('ex').rank('min').over('appno_doc_num').alias('rk')))
    per = d.group_by('rf_id').agg((pl.col('rk') == 1).all().alias('first_ex'),
                                  (pl.col('ex') <= pl.col('grant_date')).all().alias('pre_grant'))
    f = (ct.select('rf_id', 'convey_ty').join(nprop, on='rf_id', how='left')
           .join(per, on='rf_id', how='left'))
    ea = ((pl.col('convey_ty') == 'employee')
          | ((pl.col('convey_ty') == 'assignment') & (pl.col('nprop') == 1)
             & pl.col('first_ex').fill_null(True) & pl.col('pre_grant').fill_null(True)))
    return f.select('rf_id', ea.fill_null(False).cast(pl.Int8).alias('employer_assign'))


def main():
    new = pl.read_parquet(config.table('all', 'assignment'), columns=['rf_id', 'convey_text'])
    ct = convey_types(new)
    ct.select('rf_id', 'convey_text', 'convey_ty', 'step').write_parquet(config.work('convey_ty_source.parquet'))
    print('rf_ids typed by step:', dict(ct.group_by('step').len().sort('step').iter_rows()))
    for universe in config.UNIVERSES:
        ids = pl.read_parquet(config.table(universe, 'assignment'), columns=['rf_id'])
        cu = ct.join(ids, on='rf_id', how='semi')
        ea = employer_assign(cu, universe)
        out = cu.select('rf_id', 'convey_ty').join(ea, on='rf_id', how='left').sort('rf_id')
        out.write_parquet(config.table(universe, 'assignment_conveyance'))
        print(f'{universe}: {out.height:,} rows')
        with pl.Config(tbl_rows=20):
            print(out.group_by('convey_ty').agg(pl.len(), pl.col('employer_assign').mean())
                     .sort('len', descending=True))


if __name__ == '__main__':
    main()
