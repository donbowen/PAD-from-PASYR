# PAD rebuilt from the PASYR backfile

This folder rebuilds the USPTO **Patent Assignment Dataset (PAD)** from the USPTO's *Annual
Patent Assignment XML backfile* (product **PASYR**). PAD's own release (ECORSEXC, last one
dated January 2024) is no longer published. The output has the same six tables, with the same
columns, names, types and file formats as the 2023 release, so it can replace it in existing
code. It covers recordings up to the backfile's cut-off (2026-04-09 for the April 2026
backfile).

It was checked against the 2023 release on the 10.5 million transactions the two share. See
"How close is it to the 2023 release?" below.

## Getting the inputs

1. **The PASYR zips** go in `inputs_raw/`. The April 2026 backfile has 27 parts,
   `ad19000815-20260409-01.zip` to `-27.zip`, each holding one XML file (1-2 GB unzipped).
   They are listed by the USPTO Open Data Portal API (needs an API key):

   ```
   curl -X GET 'https://api.uspto.gov/api/v1/datasets/products/PASYR?fileDataFromDate=2026-07-29&fileDataToDate=2026-10-04&includeFiles=true' \
        -H 'Accept: application/json' -H 'x-api-key: <your-api-key>'
   ```

2. **PatentsView `g_application.tsv.zip`** (patent number <-> application number), used for
   `documentid_admin`. By default it is read from the folder above this one. Set the
   environment variable `PV_G_APPLICATION` to read it from somewhere else.
   Download: <https://s3.amazonaws.com/data.patentsview.org/download/g_application.tsv.zip>.

3. **Optional: the 2023 PAD release** (`assignment.dta.zip`, `assignor.csv.zip`, ...), used
   only to check the rebuild (step 06) and to rebuild the conveyance-type lookup (step 04). By
   default it is read from the folder above this one; set `PAD2023_DIR` otherwise. Without it,
   step 04 uses the lookup saved in `data/convey_ty_lookup.csv.gz`, and step 06 cannot run.

## Running it

Python 3.10+ with `polars`, `pyarrow`, `lxml` and `pandas`. Stata is optional; it is used only
to write the `.dta` files, and is found at `C:\Program Files\StataNow19\StataMP-64.exe` or
`STATA_EXE`.

```
python code/00_cache_pad2023.py      # optional: 2023 release -> work/pad2023/*.parquet
python code/01_parse_xml.py          # XML -> work/parsed/<part>/*.parquet  (~4 min, parallel)
python code/02_build_tables.py       # -> work/full/{assignment,assignor,assignee,documentid}
python code/03_universe.py           # -> outputs/ (drop-in) and outputs/all/
python code/04_conveyance.py         # -> assignment_conveyance, both versions
python code/05_documentid_admin.py   # -> documentid_admin, both versions
python code/06_validate.py           # compare outputs/ with the 2023 release
python code/07_export.py             # -> outputs/export/*.csv.zip and *.dta.zip
```

On a 24-core machine the whole run takes about 35 minutes: 4 for parsing, 10 for steps 02-06, and 20 for the Stata export in step 07.

## What you get

`outputs/export/` has the drop-in files, named as in the 2023 release:
`assignment`, `assignee`, `assignor`, `documentid`, `documentid_admin` and
`assignment_conveyance`, each as `<table>.csv.zip` and `<table>.dta.zip`. `outputs/*.parquet`
has the same tables as parquet. `outputs/all/` has the version with unpublished applications
(next section).

| table | one row per | columns |
| --- | --- | --- |
| `assignment` | transaction (`rf_id`) | `rf_id file_id cname caddress_1-4 reel_no frame_no convey_text record_dt last_update_dt page_count purge_in` |
| `assignor` | assignor of a transaction | `rf_id or_name exec_dt ack_dt` |
| `assignee` | assignee of a transaction | `rf_id ee_name ee_address_1 ee_address_2 ee_city ee_state ee_postcode ee_country` |
| `documentid` | property (application / patent) in a transaction | `rf_id title lang appno_doc_num appno_date appno_country pgpub_doc_num pgpub_date pgpub_country grant_doc_num grant_date grant_country` |
| `assignment_conveyance` | transaction | `rf_id convey_ty employer_assign` |
| `documentid_admin` | distinct (rf_id, appno, grant) | `rf_id appno_doc_num grant_doc_num admin_appl_id_for_grant admin_pat_no_for_appno error` |

`rf_id = reel_no * 10000 + frame_no`, as before.

### Two versions: published (the drop-in) and all

The XML has every recorded transaction, including about 1.55 million on applications that were
never published: provisional applications, PCT applications, and applications abandoned before
publication. The 2023 release left these out. Its `documentid` holds only properties with a
publication or patent number (plus Hague design applications, series 35), and its transactions
are those with at least one such property.

* `outputs/` (**published**, the drop-in) applies the same rule. A property counts if it has a
  publication or patent number in the 2026 XML, so applications published after January 2024
  are now included.
* `outputs/all/` keeps everything.

`employer_assign` is computed separately in each version, because it depends on how many
properties a transaction has and on which transaction comes first for a property.

## How the tables are built

**Straight from the XML** (`assignment`, `assignor`, `assignee`, `documentid`). Values are as
the XML gives them. Leading and trailing spaces are stripped, and empty strings become missing.
Each `<patent-property>` gives one `documentid` row. Its `<document-id>`s are sorted by kind
code: `X0` is the application, `A1` the publication, `B1`/`B2` the patent. These are the only
kinds in the backfile. A reel/frame that appears twice keeps its latest copy, as the 2015
working paper describes. In this backfile all 315,572 repeats are identical copies where two
consecutive parts overlap. `file_id` is the backfile part number (1-27); in 2023 it was the
part number of that release's 20 files. `purge_in` is 1 for a purged record and missing
otherwise, as in 2023; no record is purged.

**`assignment_conveyance.convey_ty`.** In the 2023 release the type is a fixed function of the
conveyance text. The USPTO has since reworded the texts, for example "ASSIGNMENT OF ASSIGNORS
INTEREST (SEE DOCUMENT FOR DETAILS)." is now "ASSIGNMENT OF ASSIGNOR'S INTEREST". So each text
takes the type the 2023 release gave the same transactions. Failing that, it takes the type
the 2023 release gave the identical text. Failing both, keyword rules apply
(`code/conveyance_rules.py`, built from the 2015 working paper's description). The first two
steps are saved in `data/convey_ty_lookup.csv.gz`. Only 0.3% of transactions (42,500) need the
rules, and on the 2023 release's rare texts the rules agree with it 98% of the time.

**`assignment_conveyance.employer_assign`.** The rule was reverse-engineered from the 2023
release, where it reproduces 99.92% of the flags. A transaction is flagged when its type is
`employee`, or when all of these hold:

* its type is `assignment`;
* it conveys one property;
* it is the earliest-executed transaction on that application;
* it was executed on or before the grant date.

**`documentid_admin`.** The 2023 release checked application/patent number pairs against
internal USPTO records. These are not public, so PatentsView's `g_application` stands in. The
`error` labels follow the 2023 rules, which reproduce 99.986% of its labels from its own
columns. One difference: PatentsView starts in 1976, so pre-1976 patents get no admin
numbers.

## How close is it to the 2023 release?

`code/06_validate.py` compares the drop-in tables with the 2023 release on the `rf_id`s in
both. Results are in `outputs/validation/*.csv`.

**Coverage** (`coverage_by_year.csv`). The drop-in has 11,537,857 transactions:

* 10,459,259 also in the 2023 release;
* 1,078,598 new, mostly recorded from 2022 on, plus older ones whose application has been
  published since;
* 72,638 in 2023 only (0.7%). See "Known differences".

**Agreement on the shared transactions** (share of transactions where the column agrees):

| table | column | agrees | the rest |
| --- | --- | ---: | --- |
| assignment | reel_no, frame_no, record_dt, page_count, purge_in | 100% | |
| assignment | cname, caddress_1-4 | 99.99999% | 1 record each |
| assignment | convey_text | 11.7% | reworded by the USPTO (see below) |
| assignment | last_update_dt, file_id | 0% / 8% | not comparable: the backfile stamps every record 2026-04-09, and the parts differ |
| assignor | rows per transaction, or_name, exec_dt | 99.98% | 2,161 transactions gained or lost an assignor since 2023 |
| assignee | rows per transaction | 100% | |
| assignee | ee_name, addresses, city, postcode | 99.9997% or more | |
| assignee | ee_state, ee_country | 99.5% | USPTO recoded places, e.g. ENGLAND -> UNITED KINGDOM |
| assignment_conveyance | convey_ty | 99.99998% | 2 transactions |
| assignment_conveyance | employer_assign | 99.80% | 21,085; see below |
| documentid | properties per transaction | 99.58% | |

**documentid property by property** (`documentid_rows.csv`, 18.36 million properties matched
on rf_id + application number):

| column | agrees | filled (2023 missing, now present) | emptied | changed |
| --- | ---: | ---: | ---: | ---: |
| grant_doc_num | 97.28% | 495,209 | 1,568 | 3,226 |
| pgpub_doc_num | 99.70% | 30,091 | 19 | 25,442 |
| appno_date | 99.997% | 93 | 47 | 367 |
| title | 99.85% | 0 | 0 | 27,336 |

Nearly all grant disagreements are patents granted after the 2023 release, so they are
updates.

**documentid_admin row by row** (`documentid_admin_rows.csv`, 17.86 million rows): PatentsView
gives the same application number as the USPTO's administrative data on 99.85% of rows. The
`error` label agrees on 99.98%.

## Known differences from the 2023 release

These come from the source, not the parsing. Each was traced to the raw XML.

* **Conveyance texts were reworded.** "ASSIGNMENT OF ASSIGNORS INTEREST (SEE DOCUMENT FOR
  DETAILS)." is now "ASSIGNMENT OF ASSIGNOR'S INTEREST"; "RELEASE BY SECURED PARTY (SEE
  DOCUMENT FOR DETAILS)." is now "RELEASE OF SECURITY INTEREST". Code that searches
  `convey_text` for words such as ASSIGN or LICENS still works. Code that matches whole 2023
  texts does not; use `convey_ty`.
* **Patents granted before about 1976 that have no bibliographic record are gone.** The 2023
  release listed about 330,000 such properties: patent number only, with no title or dates.
  The backfile omits them. As a result, 14,131 transactions whose only properties were such
  patents are not in the backfile at all.
* **PCT and provisional numbers.** For about 45,000 transactions, the backfile lists the
  property by its PCT or provisional application number where the 2023 release listed the US
  application or patent. With no publication or patent number, they fall outside the drop-in.
  They are in `outputs/all/`.
* **13,464 other 2023 transactions** are not in the backfile, for no reason visible in the
  data.
* **`employer_assign`** agrees on 99.80%. The rule reproduces 99.92% when run on the 2023
  release's own inputs. The rest comes from changes in those inputs, such as properties
  added to or removed from a transaction, and newly recorded earlier transactions.
* **`last_update_dt`** is 2026-04-09 for every record (the backfile's extraction date), so it
  no longer shows when a record last changed.
* **`documentid_admin`** uses PatentsView (see above). Pre-1976 patents have no admin numbers.
  1990s applications with PatentsView series-code errors are repaired by serial number and
  filing date.
* **Places:** the USPTO recoded some assignee states and countries (0.5%).

## Files

```
code/       the pipeline (run in number order); config.py holds every path
data/       convey_ty_lookup.csv.gz: conveyance text -> type, from the 2023 release
docs/       USPTO PADX file description (v2) and the 2015 PAD working paper (Marco et al.)
inputs_raw/ the PASYR zips (not in git)
work/       intermediate files (not in git)
outputs/    the tables (not in git, except outputs/validation/*.csv)
```
