"""
config.py -- where every PASYR script reads and writes.

    inputs_raw/        the PASYR zip files from the USPTO (one XML file each)
    docs/              USPTO file description and the 2015 PAD working paper
    work/              intermediate files (parsed XML as parquet, cached 2023 PAD release)
    outputs/           the rebuilt PAD tables, in the layout of the 2023 release

The 2023 PAD release (ECORSEXC/2023) is only used to check the rebuild. Point PAD2023_DIR at
the folder that holds its zips (assignment.dta.zip, assignor.csv.zip, ...). The default is the
folder above this one, which is where the parent project keeps them. Override it with the
environment variable PAD2023_DIR.
"""

from __future__ import annotations

import os

CODE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(CODE)

RAW = os.path.join(ROOT, 'inputs_raw')
WORK = os.path.join(ROOT, 'work')
OUT = os.path.join(ROOT, 'outputs')

PARSED = os.path.join(WORK, 'parsed')        # one set of parquet files per zip
PAD2023 = os.path.join(WORK, 'pad2023')      # 2023 release, cached as parquet
PAD2023_DIR = os.environ.get('PAD2023_DIR', os.path.dirname(ROOT))

for _d in (WORK, OUT, PARSED, PAD2023):
    os.makedirs(_d, exist_ok=True)


def work(*parts) -> str:
    p = os.path.join(WORK, *parts)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p


def out(*parts) -> str:
    p = os.path.join(OUT, *parts)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p


# Two versions of the tables (see 03_universe.py):
#   published  outputs/       the drop-in: same coverage rule as the 2023 release
#   all        outputs/all/   every transaction and property in the XML
UNIVERSES = {'published': OUT, 'all': os.path.join(OUT, 'all')}
FULL = os.path.join(WORK, 'full')  # 02_build_tables.py writes the "all" source tables here


def table(universe: str, name: str) -> str:
    """Path of outputs/[all/]<name>.parquet."""
    d = UNIVERSES[universe]
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f'{name}.parquet')


# The six tables of the 2023 release and the file each came in.
PAD2023_FILES = {
    'assignment': 'assignment.dta.zip',
    'assignee': 'assignee.csv.zip',
    'assignor': 'assignor.csv.zip',
    'documentid': 'documentid.dta.zip',
    'documentid_admin': 'documentid_admin.dta.zip',
    'assignment_conveyance': 'assignment_conveyance.dta.zip',
}
