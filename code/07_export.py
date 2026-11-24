"""
07_export.py -- write the drop-in files: the six tables as zipped CSV and zipped Stata files,
named as the 2023 PAD release named them.

    outputs/export/<table>.csv.zip   e.g. assignee.csv.zip, holding assignee.csv
    outputs/export/<table>.dta.zip   e.g. assignment.dta.zip, holding assignment.dta

Each table is the published version (outputs/<table>.parquet), with the 2023 release's column
order. In the CSV files dates are YYYY-MM-DD and missing values are empty, as in 2023. The Stata
files are made by Stata itself (batch mode) from the CSV files, with the 2023 variable labels,
dates as %td, and identifiers (appno_doc_num, grant_doc_num, ...) as strings, so leading zeros
are kept. If Stata is not found, only the CSV files are written.

Usage:  python code/07_export.py [--all] [--csv-only] [table ...]
        --all  exports outputs/all/ to outputs/export_all/ instead
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import zipfile

import polars as pl

import config

STATA = os.environ.get('STATA_EXE', r'C:\Program Files\StataNow19\StataMP-64.exe')

# Column order, Stata storage type for numbers, and labels, from the 2023 release.
SPEC = {
    'assignment': [
        ('rf_id', 'long', 'Reel Frame ID Number - unique linking variable for this dataset'),
        ('file_id', 'byte', 'US Patent Assignment ID'),
        ('cname', 'str', 'Correspondent Name'),
        ('caddress_1', 'str', 'Correspondent Address Line 1'),
        ('caddress_2', 'str', 'Correspondent Address Line 2'),
        ('caddress_3', 'str', 'Correspondent Address Line 3'),
        ('caddress_4', 'str', 'Correspondent Address Line 4'),
        ('reel_no', 'long', 'Reel Number of Microfiche Record'),
        ('frame_no', 'int', 'Frame Number of Microfiche Record'),
        ('convey_text', 'str', 'Conveyance Text - Describes Type of Assignment'),
        ('record_dt', 'date', 'Recorded Date with USPTO'),
        ('last_update_dt', 'date', 'Last Update Date'),
        ('page_count', 'int', 'Page Count of Assignment Record'),
        ('purge_in', 'float', 'Purge Indicator = 1-yes assignment deleted'),
    ],
    'assignee': [
        ('rf_id', 'long', 'Reel Frame ID Number - unique linking variable for this dataset'),
        ('ee_name', 'str', 'Patent Assignee Name = Entity Assigned TO'),
        ('ee_address_1', 'str', 'Patent Assignee Address Line 1'),
        ('ee_address_2', 'str', 'Patent Assignee Address Line 2'),
        ('ee_city', 'str', 'Patent Assignee City'),
        ('ee_state', 'str', 'Patent Assignee State'),
        ('ee_postcode', 'str', 'Patent Assignee Postal Code'),
        ('ee_country', 'str', 'Patent Assignee Country'),
    ],
    'assignor': [
        ('rf_id', 'long', 'Reel Frame ID Number - unique linking variable for this dataset'),
        ('or_name', 'str', 'Patent Assignor Name = Entity Assigned From'),
        ('exec_dt', 'date', 'Execution Date between Entities'),
        ('ack_dt', 'date', 'Acknowledgement Date'),
    ],
    'documentid': [
        ('rf_id', 'long', 'Reel Frame ID Number - unique linking variable for this dataset'),
        ('title', 'str', 'Invention Title'),
        ('lang', 'str', 'Language of Invention Title'),
        ('appno_doc_num', 'str', 'Application Document USPTO Number'),
        ('appno_date', 'date', 'Application Date'),
        ('appno_country', 'str', 'Application Country'),
        ('pgpub_doc_num', 'str', 'Pre-Grant Publication Document USPTO Number'),
        ('pgpub_date', 'date', 'Pre-Grant Publication Date'),
        ('pgpub_country', 'str', 'Pre-Grant Publication Country'),
        ('grant_doc_num', 'str', 'Granted Patent Document USPTO Number'),
        ('grant_date', 'date', 'Granted Patent Date'),
        ('grant_country', 'str', 'Granted Patent Country'),
    ],
    'assignment_conveyance': [
        ('rf_id', 'long', 'Reel Frame ID Number - unique linking variable for this dataset'),
        ('convey_ty', 'str', 'Conveyance Type'),
        ('employer_assign', 'byte', 'Employer Assignment Indicator'),
    ],
    'documentid_admin': [
        ('rf_id', 'long', 'Reel Frame ID Number - unique linking variable for this dataset'),
        ('appno_doc_num', 'str', 'Application Document USPTO Number'),
        ('grant_doc_num', 'str', 'Granted Patent Document USPTO Number'),
        ('admin_appl_id_for_grant', 'str', 'Application number for grant_doc_num (PatentsView)'),
        ('admin_pat_no_for_appno', 'str', 'Patent number for appno_doc_num (PatentsView)'),
        ('error', 'str', 'Identifier Error'),
    ],
}


def write_csv(df: pl.DataFrame, table: str, outdir: str) -> str:
    csv = os.path.join(outdir, f'{table}.csv')
    df.select([c for c, _, _ in SPEC[table]]).write_csv(csv, date_format='%Y-%m-%d', null_value='')
    zpath = csv + '.zip'
    with zipfile.ZipFile(zpath, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        z.write(csv, arcname=f'{table}.csv')
    return csv


def do_block(table: str, csv: str, dta: str) -> str:
    lines = [f'import delimited using "{csv}", clear varnames(1) stringcols(_all) '
             f'encoding("utf-8") bindquote(strict) maxquotedrows(unlimited)']
    for col, kind, label in SPEC[table]:
        if kind == 'date':
            lines += [f'gen double _d = date({col}, "YMD")', f'drop {col}',
                      f'rename _d {col}', f'recast float {col}', f'format {col} %td']
        elif kind in ('byte', 'int', 'long', 'float'):
            lines += [f'destring {col}, replace', f'recast {kind} {col}']
        lab = label.replace('"', "'")
        lines.append(f'label variable {col} "{lab}"')
    order = ' '.join(c for c, _, _ in SPEC[table])
    # Long fixed-width strings (str81-str2045) pad every row to the longest value and make the
    # file several times larger; store them as strL, as the 2023 release did.
    lines += [f'order {order}', 'compress',
              'quietly ds, has(type string)',
              'foreach v in `r(varlist)\' {',
              '    local t : type `v\'',
              '    if "`t\'" != "strL" {',
              '        if real(substr("`t\'", 4, .)) > 80 recast strL `v\'',
              '    }',
              '}',
              f'save "{dta}", replace', '']
    return '\n'.join(lines)


def main(args):
    universe = 'all' if '--all' in args else 'published'
    csv_only = '--csv-only' in args
    tables = [a for a in args if not a.startswith('--')] or list(SPEC)
    outdir = os.path.join(config.OUT, 'export' if universe == 'published' else 'export_all')
    os.makedirs(outdir, exist_ok=True)

    csvs = {}
    for t in tables:
        df = pl.read_parquet(config.table(universe, t))
        csvs[t] = write_csv(df, t, outdir)
        print(f'{t}.csv.zip: {df.height:,} rows', flush=True)

    if csv_only or not os.path.exists(STATA):
        if not csv_only:
            print(f'Stata not found at {STATA}; skipped the .dta files')
    else:
        do = os.path.join(outdir, '_export.do')
        with open(do, 'w', encoding='utf8') as f:
            f.write('set more off\n')
            for t in tables:
                f.write(do_block(t, csvs[t], os.path.join(outdir, f'{t}.dta')))
        subprocess.run([STATA, '/e', 'do', do], cwd=outdir, check=True)
        log = os.path.join(outdir, '_export.log')
        text = open(log, encoding='utf8', errors='replace').read() if os.path.exists(log) else ''
        if 'r(' in text and 'end of do-file' not in text:
            raise SystemExit(f'Stata stopped with an error; see {log}')
        for t in tables:
            dta = os.path.join(outdir, f'{t}.dta')
            if not os.path.exists(dta):
                raise SystemExit(f'{dta} was not written; see {log}')
            with zipfile.ZipFile(dta + '.zip', 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
                z.write(dta, arcname=f'{t}.dta')
            os.remove(dta)
            print(f'{t}.dta.zip written', flush=True)

    for csv in csvs.values():
        os.remove(csv)


if __name__ == '__main__':
    main(sys.argv[1:])
