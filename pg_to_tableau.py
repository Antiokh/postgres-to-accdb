#!/usr/bin/env python3
"""
Universal PostgreSQL -> Tableau Public exporter.

Supported PostgreSQL sources:
  * table or view:  --relation schema.name
  * set-returning function: --function schema.name --arg ...
  * inline SQL: --sql "SELECT ..."
  * SQL file: --sql-file query.sql

Supported output formats:
  * .accdb — recommended on Windows; streaming export, multiple tables, indexes
  * .sav   — portable binary file supported by Tableau Public; one flat table,
             loaded into memory before writing; no indexes

Examples are in README.md.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection


IDENTIFIER_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
SPSS_NAME_RE = re.compile(r"[^A-Za-z0-9_]")


class ExportError(RuntimeError):
    pass


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def quote_pg_qualified_identifier(value: str) -> str:
    """
    Safely quote a simple PostgreSQL qualified identifier.

    Unusual names containing spaces, dots inside quoted identifiers, etc. should
    be accessed with --sql instead.
    """
    parts = value.split(".")
    if not parts or any(not IDENTIFIER_PART_RE.fullmatch(part) for part in parts):
        raise ExportError(
            f"Unsafe or unsupported PostgreSQL identifier: {value!r}. "
            "Use --sql for unusual quoted identifiers."
        )
    return ".".join(f'"{part}"' for part in parts)


def parse_json_value(raw: str) -> Any:
    """
    Parse a CLI argument as JSON. If it is not valid JSON, keep it as a string.

    Examples:
      --arg 2024          -> int
      --arg true          -> bool
      --arg null          -> None
      --arg '"Beograd"'   -> str
      --arg Beograd       -> str
    """
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def parse_params(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ExportError(f"--params must be a JSON object: {exc}") from exc
    if not isinstance(value, dict):
        raise ExportError("--params must be a JSON object.")
    return value


def build_source_sql(args: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    params = parse_params(args.params)

    if args.relation:
        relation = quote_pg_qualified_identifier(args.relation)
        return f"SELECT * FROM {relation}", params

    if args.function:
        function = quote_pg_qualified_identifier(args.function)
        function_args = [parse_json_value(v) for v in args.arg]
        bind_names = []
        for index, value in enumerate(function_args):
            name = f"__fn_arg_{index}"
            if name in params:
                raise ExportError(f"Parameter collision: {name}")
            params[name] = value
            bind_names.append(f":{name}")
        return f"SELECT * FROM {function}({', '.join(bind_names)})", params

    if args.sql:
        return args.sql, params

    if args.sql_file:
        sql_path = Path(args.sql_file)
        if not sql_path.exists():
            raise ExportError(f"SQL file does not exist: {sql_path}")
        return sql_path.read_text(encoding="utf-8-sig"), params

    raise ExportError("No source selected.")


def iter_frames(
    connection: Connection,
    sql: str,
    params: Mapping[str, Any],
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    streamed = connection.execution_options(stream_results=True)
    yield from pd.read_sql_query(
        text(sql),
        streamed,
        params=dict(params),
        chunksize=chunk_size,
    )


def default_target_table(args: argparse.Namespace) -> str:
    if args.target_table:
        return args.target_table
    if args.relation:
        return args.relation.split(".")[-1]
    if args.function:
        return args.function.split(".")[-1]
    return "Extract"


def validate_access_name(name: str, kind: str) -> None:
    if not name:
        raise ExportError(f"Empty Access {kind} name.")
    if len(name) > 64:
        raise ExportError(
            f"Access {kind} name is longer than 64 characters: {name!r}. "
            "Alias it in SQL or pass a shorter --target-table."
        )


def quote_access_identifier(value: str) -> str:
    validate_access_name(value, "identifier")
    return "[" + value.replace("]", "]]") + "]"


def find_access_driver() -> str:
    try:
        import pyodbc
    except ImportError as exc:
        raise ExportError(
            "pyodbc is not installed. Run: pip install pyodbc pywin32"
        ) from exc

    candidates = [
        driver
        for driver in pyodbc.drivers()
        if "Microsoft Access Driver" in driver and "*.accdb" in driver
    ]
    if not candidates:
        installed = ", ".join(pyodbc.drivers()) or "(none)"
        raise ExportError(
            "Microsoft Access ODBC driver was not found. Python and the Access "
            "Database Engine must have matching architecture (normally 64-bit).\n"
            f"Installed ODBC drivers: {installed}"
        )
    return candidates[-1]


def create_access_database(path: Path) -> None:
    if os.name != "nt":
        raise ExportError(".accdb creation is supported only on Windows.")

    try:
        import win32com.client
    except ImportError as exc:
        raise ExportError(
            "pywin32 is not installed. Run: pip install pywin32"
        ) from exc

    errors: list[str] = []
    for provider in ("Microsoft.ACE.OLEDB.16.0", "Microsoft.ACE.OLEDB.12.0"):
        try:
            catalog = win32com.client.Dispatch("ADOX.Catalog")
            catalog.Create(f"Provider={provider};Data Source={path.resolve()};")
            try:
                catalog.ActiveConnection.Close()
            except Exception:
                pass
            return
        except Exception as exc:
            errors.append(f"{provider}: {exc}")

    raise ExportError(
        "Could not create the Access database. Install a matching Microsoft "
        "Access Database Engine.\n" + "\n".join(errors)
    )


def open_access_database(path: Path):
    import pyodbc

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        create_access_database(path)

    driver = find_access_driver()
    connection_string = f"DRIVER={{{driver}}};DBQ={path.resolve()};"
    return pyodbc.connect(connection_string, autocommit=False)


def access_table_exists(cursor: Any, table_name: str) -> bool:
    wanted = table_name.casefold()
    for row in cursor.tables(tableType="TABLE"):
        if str(row.table_name).casefold() == wanted:
            return True
    return False


def first_non_null(series: pd.Series) -> Any:
    for value in series:
        if value is None or value is pd.NA:
            continue
        try:
            missing = pd.isna(value)
            if isinstance(missing, bool) and missing:
                continue
        except (TypeError, ValueError):
            pass
        return value
    return None


def infer_access_type(series: pd.Series) -> str:
    from pandas.api import types as ptypes

    dtype = series.dtype

    if ptypes.is_bool_dtype(dtype):
        return "YESNO"
    if ptypes.is_integer_dtype(dtype):
        # DECIMAL is safer than Access INTEGER for PostgreSQL bigint values.
        return "DECIMAL(19,0)"
    if ptypes.is_float_dtype(dtype):
        return "DOUBLE"
    if ptypes.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    if ptypes.is_timedelta64_dtype(dtype):
        return "DOUBLE"

    sample = first_non_null(series)
    if sample is None:
        return "LONGTEXT"
    if isinstance(sample, bool):
        return "YESNO"
    if isinstance(sample, int) and not isinstance(sample, bool):
        return "DECIMAL(19,0)"
    if isinstance(sample, float):
        return "DOUBLE"
    if isinstance(sample, Decimal):
        return "DECIMAL(28,10)"
    if isinstance(sample, (datetime, date, time, pd.Timestamp)):
        return "DATETIME"
    if isinstance(sample, (bytes, bytearray, memoryview)):
        return "LONGBINARY"
    return "LONGTEXT"


def normalize_access_value(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None

    # Convert NumPy scalar types without importing NumPy explicitly.
    if hasattr(value, "item") and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            pass

    if isinstance(value, float) and math.isnan(value):
        return None

    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            # Access has no timestamp-with-time-zone type.
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value

    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min)

    if isinstance(value, time):
        return datetime.combine(date(1899, 12, 30), value.replace(tzinfo=None))

    if isinstance(value, pd.Timedelta):
        return value.total_seconds()

    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, ensure_ascii=False, default=str)

    if isinstance(value, uuid.UUID):
        return str(value)

    if isinstance(value, memoryview):
        return bytes(value)

    # PostgreSQL arrays, ranges, geometry wrappers, enums, etc.
    if not isinstance(
        value,
        (str, bytes, bytearray, bool, int, float, Decimal, datetime),
    ):
        return str(value)

    return value


def rows_for_access(frame: pd.DataFrame) -> Iterable[tuple[Any, ...]]:
    for row in frame.itertuples(index=False, name=None):
        yield tuple(normalize_access_value(value) for value in row)


def parse_index_spec(spec: str, generated_prefix: str, number: int) -> tuple[str, list[str]]:
    if "=" in spec:
        name, raw_columns = spec.split("=", 1)
        name = name.strip()
    else:
        name = f"{generated_prefix}_{number}"
        raw_columns = spec

    columns = [column.strip() for column in raw_columns.split(",") if column.strip()]
    if not columns:
        raise ExportError(f"Index has no columns: {spec!r}")

    validate_access_name(name, "index")
    for column in columns:
        validate_access_name(column, "column")
    return name, columns


def validate_requested_columns(
    requested: Sequence[str],
    actual_columns: Sequence[str],
    context: str,
) -> None:
    actual = {column.casefold(): column for column in actual_columns}
    missing = [column for column in requested if column.casefold() not in actual]
    if missing:
        raise ExportError(
            f"{context} refers to missing columns: {', '.join(missing)}. "
            f"Available columns: {', '.join(actual_columns)}"
        )


def create_access_indexes(
    cursor: Any,
    table_name: str,
    columns: Sequence[str],
    normal_specs: Sequence[str],
    unique_specs: Sequence[str],
    primary_key: str | None,
) -> None:
    table_sql = quote_access_identifier(table_name)

    if primary_key:
        pk_columns = [v.strip() for v in primary_key.split(",") if v.strip()]
        validate_requested_columns(pk_columns, columns, "Primary key")
        constraint_name = f"pk_{table_name}"[:64]
        cols_sql = ", ".join(quote_access_identifier(v) for v in pk_columns)
        cursor.execute(
            f"ALTER TABLE {table_sql} ADD CONSTRAINT "
            f"{quote_access_identifier(constraint_name)} PRIMARY KEY ({cols_sql})"
        )

    for number, spec in enumerate(normal_specs, start=1):
        name, index_columns = parse_index_spec(spec, f"idx_{table_name}", number)
        validate_requested_columns(index_columns, columns, f"Index {name}")
        cols_sql = ", ".join(quote_access_identifier(v) for v in index_columns)
        cursor.execute(
            f"CREATE INDEX {quote_access_identifier(name)} "
            f"ON {table_sql} ({cols_sql})"
        )

    for number, spec in enumerate(unique_specs, start=1):
        name, index_columns = parse_index_spec(
            spec, f"uidx_{table_name}", number
        )
        validate_requested_columns(index_columns, columns, f"Unique index {name}")
        cols_sql = ", ".join(quote_access_identifier(v) for v in index_columns)
        cursor.execute(
            f"CREATE UNIQUE INDEX {quote_access_identifier(name)} "
            f"ON {table_sql} ({cols_sql})"
        )


def export_accdb(
    frames: Iterator[pd.DataFrame],
    output: Path,
    table_name: str,
    if_exists: str,
    index_specs: Sequence[str],
    unique_index_specs: Sequence[str],
    primary_key: str | None,
) -> int:
    validate_access_name(table_name, "table")

    connection = open_access_database(output)
    cursor = connection.cursor()
    total_rows = 0
    created = False
    columns: list[str] = []

    try:
        exists = access_table_exists(cursor, table_name)

        if exists and if_exists == "fail":
            raise ExportError(
                f"Table {table_name!r} already exists in {output}. "
                "Use --if-exists replace or append."
            )
        if exists and if_exists == "replace":
            cursor.execute(f"DROP TABLE {quote_access_identifier(table_name)}")
            connection.commit()
            exists = False
        if exists and if_exists == "append":
            # Read the real Access column order.
            columns = [
                str(row.column_name)
                for row in cursor.columns(table=table_name)
            ]
            created = True

        for frame_number, frame in enumerate(frames, start=1):
            frame_columns = [str(column) for column in frame.columns]

            if len({column.casefold() for column in frame_columns}) != len(frame_columns):
                raise ExportError(
                    "Access column names must be unique case-insensitively. "
                    "Alias duplicate columns in SQL."
                )
            for column in frame_columns:
                validate_access_name(column, "column")

            if not created:
                columns = frame_columns
                definitions = ", ".join(
                    f"{quote_access_identifier(column)} "
                    f"{infer_access_type(frame[column])}"
                    for column in columns
                )
                cursor.execute(
                    f"CREATE TABLE {quote_access_identifier(table_name)} "
                    f"({definitions})"
                )
                connection.commit()
                created = True
            else:
                if [c.casefold() for c in columns] != [
                    c.casefold() for c in frame_columns
                ]:
                    raise ExportError(
                        "The source columns do not match the target Access table. "
                        f"Target: {columns}; source: {frame_columns}"
                    )

            if frame.empty:
                continue

            placeholders = ", ".join("?" for _ in columns)
            columns_sql = ", ".join(quote_access_identifier(c) for c in columns)
            insert_sql = (
                f"INSERT INTO {quote_access_identifier(table_name)} "
                f"({columns_sql}) VALUES ({placeholders})"
            )

            cursor.executemany(insert_sql, rows_for_access(frame))
            connection.commit()
            total_rows += len(frame)
            print(
                f"\rExported {total_rows:,} rows to "
                f"{output.name}:{table_name}",
                end="",
                flush=True,
            )

        if not created:
            raise ExportError("The query produced no columns.")

        # Indexes are created after loading; maintaining them during bulk insert is slower.
        if if_exists != "append":
            create_access_indexes(
                cursor=cursor,
                table_name=table_name,
                columns=columns,
                normal_specs=index_specs,
                unique_specs=unique_index_specs,
                primary_key=primary_key,
            )
            connection.commit()
        elif index_specs or unique_index_specs or primary_key:
            eprint(
                "\nIndexes were not changed in append mode. "
                "Create them during the initial replace/fail export."
            )

        print()
        return total_rows
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def make_spss_name(original: str, used: set[str], position: int) -> str:
    candidate = SPSS_NAME_RE.sub("_", original).strip("_")
    if not candidate or not candidate[0].isalpha():
        candidate = f"v_{candidate}" if candidate else f"v_{position}"
    candidate = candidate[:64]

    base = candidate
    suffix = 1
    while candidate.casefold() in used:
        suffix_text = f"_{suffix}"
        candidate = base[: 64 - len(suffix_text)] + suffix_text
        suffix += 1

    used.add(candidate.casefold())
    return candidate


def prepare_sav_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    used: set[str] = set()
    rename: dict[str, str] = {}
    labels: dict[str, str] = {}

    for position, column in enumerate(frame.columns, start=1):
        original = str(column)
        safe = make_spss_name(original, used, position)
        rename[original] = safe
        if safe != original:
            labels[safe] = original

    frame = frame.rename(columns=rename).copy()

    # SPSS numeric storage is double precision. Keep datetime columns; convert
    # unsupported objects (JSON, UUID, arrays, geometry wrappers) to strings.
    for column in frame.columns:
        series = frame[column]
        if pd.api.types.is_integer_dtype(series.dtype) and series.isna().any():
            frame[column] = series.astype("float64")
        elif series.dtype == "object":
            frame[column] = series.map(
                lambda value: None
                if value is None or value is pd.NA
                else (
                    json.dumps(value, ensure_ascii=False, default=str)
                    if isinstance(value, (dict, list, tuple, set))
                    else str(value)
                    if isinstance(value, (uuid.UUID, Decimal))
                    else value
                )
            )

    return frame, labels


def export_sav(
    frames: Iterator[pd.DataFrame],
    output: Path,
    if_exists: str,
    index_specs: Sequence[str],
    unique_index_specs: Sequence[str],
    primary_key: str | None,
) -> int:
    if index_specs or unique_index_specs or primary_key:
        raise ExportError(
            ".sav files do not support user-defined indexes or primary keys. "
            "Use .accdb if you need them."
        )

    if output.exists() and if_exists == "fail":
        raise ExportError(
            f"Output already exists: {output}. Use --if-exists replace."
        )
    if if_exists == "append":
        raise ExportError(".sav append mode is not supported.")

    try:
        import pyreadstat
    except ImportError as exc:
        raise ExportError(
            "pyreadstat is not installed. Run: pip install pyreadstat"
        ) from exc

    chunks: list[pd.DataFrame] = []
    total_rows = 0
    for frame in frames:
        chunks.append(frame)
        total_rows += len(frame)
        print(
            f"\rRead {total_rows:,} rows from PostgreSQL",
            end="",
            flush=True,
        )
    print()

    if not chunks:
        raise ExportError("The query produced no result set.")

    frame = pd.concat(chunks, ignore_index=True)
    frame, labels = prepare_sav_frame(frame)

    output.parent.mkdir(parents=True, exist_ok=True)
    pyreadstat.write_sav(
        frame,
        str(output),
        column_labels=labels or None,
        row_compress=True,
    )
    print(f"Wrote {total_rows:,} rows to {output}")
    return total_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL data for Tableau Public.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Source examples:
  --relation public.traffic_accidents
  --function public.get_accidents --arg 2024 --arg '"Beograd"'
  --sql "SELECT * FROM public.traffic_accidents WHERE year = :year" --params '{"year": 2024}'
  --sql-file query.sql --params '{"year": 2024}'

Access index syntax:
  --primary-key accident_id
  --index idx_year=year
  --index idx_municipality_year=municipality_formatted,year
  --unique-index uid_external=external_id
""",
    )

    parser.add_argument(
        "--dsn",
        default=os.getenv("PG_DSN"),
        help=(
            "SQLAlchemy PostgreSQL URL. Defaults to PG_DSN environment variable, "
            "for example postgresql+psycopg://user:password@host:5432/database"
        ),
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--relation", help="PostgreSQL table or view: schema.name")
    source.add_argument("--function", help="Set-returning function: schema.name")
    source.add_argument("--sql", help="Inline SQL query")
    source.add_argument("--sql-file", help="UTF-8 SQL file")

    parser.add_argument(
        "--arg",
        action="append",
        default=[],
        help="Positional function argument. Repeat as needed; parsed as JSON when possible.",
    )
    parser.add_argument(
        "--params",
        help='JSON object for named SQL parameters, e.g. \'{"year": 2024}\'',
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output .accdb or .sav file",
    )
    parser.add_argument(
        "--target-table",
        help="Target table inside .accdb. Default: source name or Extract.",
    )
    parser.add_argument(
        "--if-exists",
        choices=("fail", "replace", "append"),
        default="replace",
        help="Target handling. Default: replace.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Rows fetched per PostgreSQL chunk. Default: 5000.",
    )

    parser.add_argument(
        "--primary-key",
        help="Access primary-key columns, comma-separated.",
    )
    parser.add_argument(
        "--index",
        action="append",
        default=[],
        help="Access index: [name=]column1,column2. Repeatable.",
    )
    parser.add_argument(
        "--unique-index",
        action="append",
        default=[],
        help="Access unique index: [name=]column1,column2. Repeatable.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if not args.dsn:
            raise ExportError(
                "PostgreSQL DSN is missing. Pass --dsn or set PG_DSN."
            )
        if args.chunk_size <= 0:
            raise ExportError("--chunk-size must be greater than zero.")
        if args.arg and not args.function:
            raise ExportError("--arg is valid only with --function.")

        output = Path(args.output).expanduser().resolve()
        suffix = output.suffix.casefold()
        if suffix not in {".accdb", ".sav"}:
            raise ExportError("--output must end with .accdb or .sav")

        sql, params = build_source_sql(args)
        target_table = default_target_table(args)

        engine = create_engine(args.dsn, pool_pre_ping=True)
        try:
            with engine.connect() as connection:
                frames = iter_frames(
                    connection=connection,
                    sql=sql,
                    params=params,
                    chunk_size=args.chunk_size,
                )

                if suffix == ".accdb":
                    export_accdb(
                        frames=frames,
                        output=output,
                        table_name=target_table,
                        if_exists=args.if_exists,
                        index_specs=args.index,
                        unique_index_specs=args.unique_index,
                        primary_key=args.primary_key,
                    )
                else:
                    export_sav(
                        frames=frames,
                        output=output,
                        if_exists=args.if_exists,
                        index_specs=args.index,
                        unique_index_specs=args.unique_index,
                        primary_key=args.primary_key,
                    )
        finally:
            engine.dispose()

        return 0
    except ExportError as exc:
        eprint(f"ERROR: {exc}")
        return 2
    except KeyboardInterrupt:
        eprint("\nCancelled.")
        return 130
    except Exception as exc:
        eprint(f"UNEXPECTED ERROR: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
