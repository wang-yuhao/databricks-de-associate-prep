"""Course 2 helpers: simulated Unity Catalog identities, PII discovery, and k-anonymity checks.

Unity Catalog evaluates `is_account_group_member('group')` against the *querying* user. Locally there is
one user, so `use_identity()` re-registers that function for whichever persona you want to simulate.
Row-filter and column-mask functions written against it are identical to their Databricks versions.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PERSONAS = {
    "admin": ("admin@shop.example", {"admins", "pii_readers", "eu_analysts", "us_analysts"}),
    "eu_analyst": ("eva@shop.example", {"eu_analysts"}),
    "us_analyst": ("sam@shop.example", {"us_analysts"}),
    "dpo": ("dpo@shop.example", {"pii_readers", "eu_analysts", "us_analysts"}),  # data protection officer
    "contractor": ("contractor@partner.example", set()),
}


@dataclass
class Identity:
    user: str
    groups: set[str] = field(default_factory=set)


_current = Identity(*PERSONAS["admin"])


def use_identity(spark: SparkSession, persona: str | None = None, *, user: str | None = None,
                 groups: set[str] | None = None) -> Identity:
    """Switch the simulated querying user. Re-registers `is_account_group_member()` and `lab_current_user()`."""
    global _current
    if persona:
        user, groups = PERSONAS[persona]
    _current = Identity(user, set(groups or set()))
    member_of = frozenset(_current.groups)
    name = _current.user
    spark.udf.register("is_account_group_member", lambda g: g in member_of, "boolean")
    spark.udf.register("is_member", lambda g: g in member_of, "boolean")  # legacy workspace-group variant
    # current_user() is a Spark built-in and cannot be overridden; use lab_current_user() locally.
    spark.udf.register("lab_current_user", lambda: name, "string")
    print(f"Now querying as {name}  groups={sorted(member_of) or '[]'}")
    return _current


def current_identity() -> Identity:
    return _current


# --------------------------------------------------------------------------- PII discovery

# Most specific first: on a tie, the first pattern wins (an SSN or a date also looks like a phone number).
PII_PATTERNS = {
    "email": r"^[\w.+-]+@[\w-]+\.[\w.]+$",
    "ssn": r"^\d{3}-\d{2}-\d{4}$",
    "ip_address": r"^(\d{1,3}\.){3}\d{1,3}$",
    "date_of_birth": r"^(19|20)\d{2}-\d{2}-\d{2}$",
    "credit_card": r"^(?:\d[ -]?){13,19}$",
    "phone": r"^\+?[\d][\d\s().-]{6,}$",
}
NAME_HINTS = {"first_name": "name", "last_name": "name", "name": "name", "street": "address",
              "address": "address", "zip": "postal_code", "postcode": "postal_code", "dob": "date_of_birth",
              "birth": "date_of_birth", "salary": "financial", "gender": "sensitive_attribute"}


def scan_pii(df: DataFrame, sample: int = 500, threshold: float = 0.8) -> DataFrame:
    """Classify columns as likely PII from their names and a sample of values (like UC Data Classification)."""
    rows = [r.asDict() for r in df.limit(sample).collect()]
    spark = df.sparkSession
    out = []
    for col in df.columns:
        values = [str(r[col]) for r in rows if r[col] is not None]
        hits = {k: sum(bool(re.match(p, v)) for v in values) / len(values) for k, p in PII_PATTERNS.items()} \
            if values else {}
        best = max(hits, key=hits.get) if hits else None
        by_value = best if best and hits[best] >= threshold else None
        by_name = next((tag for hint, tag in NAME_HINTS.items() if hint in col.lower()), None)
        tag = by_value or by_name
        out.append((col, tag, "value pattern" if by_value else ("column name" if by_name else None),
                    round(hits.get(best, 0.0), 2) if by_value else None))
    return spark.createDataFrame(out, "column STRING, pii_class STRING, detected_by STRING, match_ratio DOUBLE")


# --------------------------------------------------------------------------- k-anonymity


def k_anonymity(df: DataFrame, quasi_identifiers: list[str]) -> int:
    """The k of a dataset: size of the smallest group sharing the same quasi-identifier values."""
    row = df.groupBy(*quasi_identifiers).count().agg(F.min("count").alias("k")).first()
    return int(row["k"] or 0)


def equivalence_classes(df: DataFrame, quasi_identifiers: list[str]) -> DataFrame:
    return df.groupBy(*quasi_identifiers).count().orderBy("count")
