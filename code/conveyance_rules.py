"""
conveyance_rules.py -- keyword rules that put a conveyance text into a PAD conveyance type.

Used only for texts the 2023 PAD release never classified (see 03_conveyance.py); every other
text takes the type the 2023 release gave it. The rules follow the 2015 working paper's
description (its footnote 40): strip special characters, search for key terms, and settle texts
that hit several types by precedence -- correction first, then release, merger over name change,
employer assignment over assignment and security interest. The term lists were then tuned
against the 2023 release's own classification of its 232,000 distinct texts; 05_validate.py
reports how often the rules agree with it.

Types (the 2023 release's codes): correct, release, security, merger, namechg, govern, employee,
assignment, other, missing ("missing" = no key term found, or no text).
"""

from __future__ import annotations

import polars as pl

# (type, regex on the normalised text). First match wins.
RULES: list[tuple[str, str]] = [
    ('correct', r'CORRECT|\bERROR|\bERRONEOUS|CONVERSION|\bCONVERT|AMENDMENT|\bAMENDED\b|\bRE ?RECORD'
                r'|SUPPLEMENT|DUPLICATE|\bSEE RECORDING'),
    ('release', r'RELEASE|\bTERMINAT|\bDISCHARGE|SATISFACTION OF'),
    ('merger', r'\bMERGE|AMALGAMAT'),
    ('employee', r'EMPLOY'),
    ('namechg', r'CHANGE OF NAME|NAME CHANGE|CHANGE IN NAME|CHANGED NAME|CHANGE OF (THE )?ASSIGNEE ?S? NAME|ADDRESS'
                r'|CHANGE OF LEGAL FORM|CHANGE OF CORPORATE FORM'),
    ('govern', r'CONFIRMATORY|GOVERNMENT INTEREST|EXECUTIVE ORDER'),
    ('security', r'SECURITY|\bLIEN|MORTGAGE|PLEDGE|COLLATERAL|\bPURCHASE AGREEMENT|\bOPTION'),
    ('other', r'LICENS|\bCOURT\b|\bLETTERS\b|BANKRUPT|PROBATE|JUDGMENT|JUDGEMENT|\bDECREE|\bWILL\b|TESTAMENT'),
    ('assignment', r'ASSIGN|NUNC PRO TUNC|BILL OF SALE|QUITCLAIM|\bTRANSFER|OWNERSHIP|CONTRIBUTION|\bDEED\b|RESCI|REVOCATION'),
]


def normalise(col: pl.Expr) -> pl.Expr:
    return col.str.to_uppercase().str.replace_all(r'[^A-Z0-9]+', ' ').str.strip_chars()


def classify(col: pl.Expr) -> pl.Expr:
    """Conveyance type of a text column, by the rules above."""
    x = normalise(col)
    expr = pl.when(x.is_null() | (x == '')).then(pl.lit('missing'))
    for ty, rx in RULES:
        expr = expr.when(x.str.contains(rx)).then(pl.lit(ty))
    return expr.otherwise(pl.lit('missing'))
