#!/usr/bin/env python3
"""Idempotently patch the Trino SQLAlchemy dialect (trino==0.339.0).

Bug: for UNPARTITIONED Iceberg tables, ``table$partitions`` exposes only the
aggregate metadata columns (record_count, file_count, total_size, data). The
vendor code only recognises the *partitioned* Iceberg shape (which additionally
has a leading ``partition`` row column), so those four metadata columns leak
back as "partition names". Superset's SQL Lab then builds bogus data-preview
queries like ``WHERE record_count = 80751`` that Trino rejects with
"Column 'record_count' cannot be resolved".

This script adds an ``is_iceberg_unpartitioned`` check so the aggregate-only
shape is also treated as Iceberg (returns None => not Hive partitions).

Safe to run on every container start: exits 0 immediately if already applied
or if a newer (fixed) driver is installed that no longer contains the old
block.

Usage: python trino_iceberg_partitions.py [path/to/dialect.py]
"""

import importlib.util
import os
import sys
import tempfile

OLD = """        # Compare the column names and types to the shape of an Iceberg $partitions table
        if (partition_names == ['partition', 'record_count', 'file_count', 'total_size', 'data']
                and data_types[0].startswith('row(')
                and data_types[1] == 'bigint'
                and data_types[2] == 'bigint'
                and data_types[3] == 'bigint'
                and data_types[4].startswith('row(')):
            # This is an Iceberg $partitions table - these match the partition metadata columns
            return None
"""

NEW = """        # Compare the column names and types to the shape of an Iceberg $partitions table
        is_iceberg = (
            partition_names == ['partition', 'record_count', 'file_count', 'total_size', 'data']
            and data_types[0].startswith('row(')
            and data_types[1] == 'bigint'
            and data_types[2] == 'bigint'
            and data_types[3] == 'bigint'
            and data_types[4].startswith('row(')
        )
        # Unpartitioned Iceberg tables expose only the aggregate columns in
        # $partitions (no leading `partition` row column). Treat that shape the
        # same: nothing is a real Hive partition column.
        is_iceberg_unpartitioned = (
            partition_names == ['record_count', 'file_count', 'total_size', 'data']
            and data_types[0] == 'bigint'
            and data_types[1] == 'bigint'
            and data_types[2] == 'bigint'
            and data_types[3].startswith('row(')
        )
        if is_iceberg or is_iceberg_unpartitioned:
            # This is an Iceberg $partitions table - these match the partition metadata columns
            return None
"""

MARKER = "is_iceberg_unpartitioned"


def find_dialect() -> str:
    """Locate the installed trino.sqlalchemy.dialect module file."""
    if len(sys.argv) > 1:
        return os.path.abspath(sys.argv[1])
    if (spec := importlib.util.find_spec("trino.sqlalchemy.dialect")) and spec.origin:
        return spec.origin
    for root in sys.path:
        cand = os.path.join(root, "trino", "sqlalchemy", "dialect.py")
        if os.path.isfile(cand):
            return cand
    raise SystemExit("trino.sqlalchemy.dialect not found")


def main() -> int:
    path = find_dialect()
    with open(path, encoding="utf-8") as fh:
        source = fh.read()

    if MARKER in source:
        print(f"[patches] {path}: already patched (skip)")
        return 0
    if OLD not in source:
        print(
            f"[patches] {path}: expected vendor block not found "
            "(driver already fixed/upgraded - remove this step when upgrading trino)",
            file=sys.stderr,
        )
        return 0 if "record_count" not in source else 1

    patched = source.replace(OLD, NEW, 1)
    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".py")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(patched)
        compile(patched, path, "exec")  # syntax check before replacing
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    print(f"[patches] {path}: applied Iceberg $partitions fix")
    return 0


if __name__ == "__main__":
    sys.exit(main())