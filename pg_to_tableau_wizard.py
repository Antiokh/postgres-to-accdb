#!/usr/bin/env python3
"""
Interactive PostgreSQL -> Tableau exporter.

Default behavior starts a small Tkinter wizard:
1. Enter PostgreSQL connection details.
2. Connect and choose a table.
3. Choose columns and PostgreSQL indexes with checkmarks.
4. Export to a timestamped .accdb file with a progress bar.

For automated validation, use --headless with the connection and table options.
"""

from __future__ import annotations

import argparse
import json
import lzma
import math
import os
import queue
import re
import threading
import time as time_module
import traceback
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine, URL

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
except Exception:  # pragma: no cover - headless mode still works without Tk
    tk = None
    filedialog = None
    messagebox = None
    ttk = None


IDENTIFIER_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
CONFIG_PATH = Path.home() / ".pg_to_tableau_wizard.json"


class ExportError(RuntimeError):
    pass


@dataclass(frozen=True)
class TableRef:
    schema: str
    name: str

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True)
class TableIndex:
    name: str
    columns: tuple[str, ...]
    unique: bool = False
    primary: bool = False

    @property
    def label(self) -> str:
        parts = [self.name, f"({', '.join(self.columns)})"]
        if self.primary:
            parts.append("[primary key]")
        elif self.unique:
            parts.append("[unique]")
        return " ".join(parts)


def normalize_host(host: str) -> str:
    host = host.strip()
    if host.startswith("[") and host.endswith("]"):
        return host[1:-1]
    return host


def build_engine(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> Engine:
    if not host.strip():
        raise ExportError("PostgreSQL host is required.")
    url = URL.create(
        "postgresql+psycopg",
        username=username or None,
        password=password,
        host=normalize_host(host),
        port=port,
        database=database or None,
    )
    return create_engine(url, pool_pre_ping=True)


def quote_pg_identifier(value: str) -> str:
    if not IDENTIFIER_PART_RE.fullmatch(value):
        raise ExportError(
            f"Unsupported PostgreSQL identifier {value!r}. "
            "Use ordinary unquoted table and column names."
        )
    return f'"{value}"'


def quote_pg_table(table: TableRef) -> str:
    return f"{quote_pg_identifier(table.schema)}.{quote_pg_identifier(table.name)}"


def list_user_tables(engine: Engine) -> list[TableRef]:
    inspector = inspect(engine)
    tables: list[TableRef] = []
    for schema in inspector.get_schema_names():
        if schema in {"information_schema", "pg_catalog"} or schema.startswith("pg_"):
            continue
        for table_name in inspector.get_table_names(schema=schema):
            tables.append(TableRef(schema=schema, name=table_name))
    return sorted(tables, key=lambda item: (item.schema.casefold(), item.name.casefold()))


def load_table_metadata(engine: Engine, table: TableRef) -> tuple[list[str], list[TableIndex]]:
    inspector = inspect(engine)
    columns = [str(column["name"]) for column in inspector.get_columns(table.name, schema=table.schema)]
    if not columns:
        raise ExportError(f"No columns found for table {table.qualified_name}.")

    indexes: list[TableIndex] = []
    pk = inspector.get_pk_constraint(table.name, schema=table.schema) or {}
    pk_columns = tuple(str(value) for value in pk.get("constrained_columns") or [] if value)
    if pk_columns:
        indexes.append(TableIndex(name=pk.get("name") or f"pk_{table.name}", columns=pk_columns, primary=True))

    unique_names = {item.get("name") for item in inspector.get_unique_constraints(table.name, schema=table.schema)}
    for item in inspector.get_indexes(table.name, schema=table.schema):
        name = str(item.get("name") or "")
        index_columns = tuple(str(value) for value in item.get("column_names") or [] if value)
        if not name or not index_columns:
            continue
        if name == pk.get("name"):
            continue
        indexes.append(
            TableIndex(
                name=name,
                columns=index_columns,
                unique=bool(item.get("unique")) or name in unique_names,
            )
        )

    unique_constraints = inspector.get_unique_constraints(table.name, schema=table.schema)
    known_names = {item.name for item in indexes}
    for item in unique_constraints:
        name = str(item.get("name") or "")
        unique_columns = tuple(str(value) for value in item.get("column_names") or [] if value)
        if not name or not unique_columns or name in known_names or name == pk.get("name"):
            continue
        indexes.append(TableIndex(name=name, columns=unique_columns, unique=True))

    indexes.sort(key=lambda item: (not item.primary, item.name.casefold()))
    return columns, indexes


def count_rows(connection: Connection, table: TableRef) -> int:
    sql = text(f"SELECT COUNT(*) FROM {quote_pg_table(table)}")
    return int(connection.execute(sql).scalar_one())


def load_saved_settings() -> dict[str, str]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        value = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(value, dict):
        return {}
    return {
        key: str(raw)
        for key, raw in value.items()
        if key in {"host", "port", "database", "username", "format", "compress"}
    }


def save_settings(
    *,
    host: str,
    port: str,
    database: str,
    username: str,
    output_format: str,
    compress_output: bool,
) -> None:
    CONFIG_PATH.write_text(
        json.dumps(
            {
                "host": host,
                "port": port,
                "database": database,
                "username": username,
                "format": output_format,
                "compress": compress_output,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def iter_source_frames(
    connection: Connection,
    table: TableRef,
    columns: Sequence[str],
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    if not columns:
        raise ExportError("Select at least one column.")
    columns_sql = ", ".join(quote_pg_identifier(column) for column in columns)
    sql = text(f"SELECT {columns_sql} FROM {quote_pg_table(table)}")
    streamed = connection.execution_options(stream_results=True)
    yield from pd.read_sql_query(sql, streamed, chunksize=chunk_size)


def validate_access_name(name: str, kind: str) -> None:
    if not name:
        raise ExportError(f"Empty Access {kind} name.")
    if len(name) > 64:
        raise ExportError(f"Access {kind} name is longer than 64 characters: {name!r}")


def quote_access_identifier(value: str) -> str:
    validate_access_name(value, "identifier")
    return "[" + value.replace("]", "]]") + "]"


def find_access_driver() -> str:
    try:
        import pyodbc
    except ImportError as exc:
        raise ExportError("pyodbc is not installed.") from exc

    candidates = [
        driver
        for driver in pyodbc.drivers()
        if "Microsoft Access Driver" in driver and "*.accdb" in driver
    ]
    if not candidates:
        installed = ", ".join(pyodbc.drivers()) or "(none)"
        raise ExportError(
            "Microsoft Access ODBC driver was not found. "
            f"Installed ODBC drivers: {installed}"
        )
    return candidates[-1]


def create_access_database(path: Path) -> None:
    if os.name != "nt":
        raise ExportError(".accdb creation is supported only on Windows.")

    try:
        import win32com.client
    except ImportError as exc:
        raise ExportError("pywin32 is not installed.") from exc

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

    raise ExportError("Could not create the Access database.\n" + "\n".join(errors))


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


def infer_access_integer_type(series: pd.Series) -> str:
    non_null = series.dropna()
    if non_null.empty:
        return "INTEGER"

    try:
        minimum = int(non_null.min())
        maximum = int(non_null.max())
    except (TypeError, ValueError, OverflowError):
        return "NUMERIC(19,0)"

    if -(2**31) <= minimum and maximum <= 2**31 - 1:
        return "INTEGER"
    return "NUMERIC(19,0)"


def infer_access_type(series: pd.Series, *, force_short_text: bool = False) -> str:
    from pandas.api import types as ptypes

    dtype = series.dtype
    if ptypes.is_bool_dtype(dtype):
        return "BIT"
    if ptypes.is_integer_dtype(dtype):
        return infer_access_integer_type(series)
    if ptypes.is_float_dtype(dtype):
        return "DOUBLE"
    if ptypes.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    if ptypes.is_timedelta64_dtype(dtype):
        return "DOUBLE"

    sample = first_non_null(series)
    if sample is None:
        return "VARCHAR(255)" if force_short_text else "LONGTEXT"
    if isinstance(sample, bool):
        return "BIT"
    if isinstance(sample, int) and not isinstance(sample, bool):
        return "INTEGER" if -(2**31) <= sample <= 2**31 - 1 else "NUMERIC(19,0)"
    if isinstance(sample, float):
        return "DOUBLE"
    if isinstance(sample, Decimal):
        return "NUMERIC(28,10)"
    if isinstance(sample, (datetime, date, time, pd.Timestamp)):
        return "DATETIME"
    if isinstance(sample, (bytes, bytearray, memoryview)):
        return "LONGBINARY"
    return "VARCHAR(255)" if force_short_text else "LONGTEXT"


def normalize_access_value(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray, memoryview)):
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
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min)
    if isinstance(value, time):
        return datetime.combine(date(1899, 12, 30), value.replace(tzinfo=None))
    if isinstance(value, pd.Timedelta):
        return value.total_seconds()
    if isinstance(value, (dict, list, tuple, set)):
        return str(value)
    if isinstance(value, memoryview):
        return bytes(value)
    if not isinstance(value, (str, bytes, bytearray, bool, int, float, Decimal, datetime)):
        return str(value)
    return value


def rows_for_access(frame: pd.DataFrame) -> Iterable[tuple[Any, ...]]:
    for row in frame.itertuples(index=False, name=None):
        yield tuple(normalize_access_value(value) for value in row)


def create_access_indexes(
    cursor: Any,
    *,
    table_name: str,
    available_columns: Sequence[str],
    indexes: Sequence[TableIndex],
) -> None:
    table_sql = quote_access_identifier(table_name)
    column_set = {value.casefold() for value in available_columns}

    for item in indexes:
        for column in item.columns:
            if column.casefold() not in column_set:
                raise ExportError(f"Index {item.name} refers to missing column {column!r}.")

        cols_sql = ", ".join(quote_access_identifier(column) for column in item.columns)
        if item.primary:
            constraint_name = item.name[:64]
            cursor.execute(
                f"ALTER TABLE {table_sql} ADD CONSTRAINT "
                f"{quote_access_identifier(constraint_name)} PRIMARY KEY ({cols_sql})"
            )
        elif item.unique:
            cursor.execute(
                f"CREATE UNIQUE INDEX {quote_access_identifier(item.name)} "
                f"ON {table_sql} ({cols_sql})"
            )
        else:
            cursor.execute(
                f"CREATE INDEX {quote_access_identifier(item.name)} "
                f"ON {table_sql} ({cols_sql})"
            )


def export_table_to_access(
    *,
    engine: Engine,
    table: TableRef,
    selected_columns: Sequence[str],
    selected_indexes: Sequence[TableIndex],
    output_path: Path,
    chunk_size: int,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> int:
    table_name = table.name
    validate_access_name(table_name, "table")

    with engine.connect() as source_connection:
        total_rows = count_rows(source_connection, table)
        frames = iter_source_frames(source_connection, table, selected_columns, chunk_size)

        access_connection = open_access_database(output_path)
        cursor = access_connection.cursor()
        exported_rows = 0
        created = False
        created_columns: list[str] = []
        indexed_columns = {
            column.casefold()
            for index in selected_indexes
            for column in index.columns
        }

        try:
            if access_table_exists(cursor, table_name):
                cursor.execute(f"DROP TABLE {quote_access_identifier(table_name)}")
                access_connection.commit()

            for frame in frames:
                frame_columns = [str(column) for column in frame.columns]
                for column in frame_columns:
                    validate_access_name(column, "column")

                if not created:
                    created_columns = frame_columns
                    definitions = ", ".join(
                        f"{quote_access_identifier(column)} "
                        f"{infer_access_type(frame[column], force_short_text=column.casefold() in indexed_columns)}"
                        for column in created_columns
                    )
                    cursor.execute(
                        f"CREATE TABLE {quote_access_identifier(table_name)} ({definitions})"
                    )
                    access_connection.commit()
                    created = True

                if frame.empty:
                    continue

                placeholders = ", ".join("?" for _ in created_columns)
                columns_sql = ", ".join(quote_access_identifier(column) for column in created_columns)
                cursor.executemany(
                    f"INSERT INTO {quote_access_identifier(table_name)} ({columns_sql}) VALUES ({placeholders})",
                    rows_for_access(frame),
                )
                access_connection.commit()
                exported_rows += len(frame)

                if progress_callback:
                    progress_callback(exported_rows, total_rows, f"Exported {exported_rows:,} / {total_rows:,} rows")

            if not created:
                raise ExportError("The source query returned no columns.")

            create_access_indexes(
                cursor,
                table_name=table_name,
                available_columns=created_columns,
                indexes=selected_indexes,
            )
            access_connection.commit()

            if progress_callback:
                progress_callback(exported_rows, total_rows, f"Finished: {exported_rows:,} rows")
            return exported_rows
        except Exception:
            access_connection.rollback()
            raise
        finally:
            cursor.close()
            access_connection.close()


def prepare_sav_frame(frame: pd.DataFrame) -> pd.DataFrame:
    sav_frame = frame.copy()
    for column in sav_frame.columns:
        series = sav_frame[column]
        if pd.api.types.is_integer_dtype(series.dtype) and series.isna().any():
            sav_frame[column] = series.astype("float64")
        elif series.dtype == "object":
            sav_frame[column] = series.map(
                lambda value: None
                if value is None or value is pd.NA
                else str(value)
                if not isinstance(value, (str, bytes, bytearray, int, float, bool, Decimal, datetime, date, time))
                else value
            )
    return sav_frame


def export_table_to_sav(
    *,
    engine: Engine,
    table: TableRef,
    selected_columns: Sequence[str],
    output_path: Path,
    chunk_size: int,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> int:
    try:
        import pyreadstat
    except ImportError as exc:
        raise ExportError("pyreadstat is not installed.") from exc

    with engine.connect() as source_connection:
        total_rows = count_rows(source_connection, table)
        frames = iter_source_frames(source_connection, table, selected_columns, chunk_size)

        chunks: list[pd.DataFrame] = []
        exported_rows = 0
        for frame in frames:
            chunks.append(frame)
            exported_rows += len(frame)
            if progress_callback:
                progress_callback(exported_rows, total_rows, f"Read {exported_rows:,} / {total_rows:,} rows")

        if not chunks:
            raise ExportError("The source query returned no result set.")

        merged = pd.concat(chunks, ignore_index=True)
        merged = prepare_sav_frame(merged)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pyreadstat.write_sav(merged, str(output_path), row_compress=True)
        if progress_callback:
            progress_callback(exported_rows, total_rows, f"Finished: {exported_rows:,} rows")
        return exported_rows


def make_timestamped_output_name(table_name: str, output_format: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", table_name).strip("_") or "export"
    return f"{safe_name}_{stamp}.{output_format}"


def compress_with_py7zr(
    source_path: Path,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> Path:
    try:
        import py7zr
    except ImportError as exc:
        raise ExportError("py7zr is not installed.") from exc

    archive_path = source_path.with_suffix(source_path.suffix + ".7z")
    source_size = max(1, source_path.stat().st_size)
    if archive_path.exists():
        archive_path.unlink()

    done = threading.Event()
    failure: list[BaseException] = []

    def worker() -> None:
        try:
            filters = [
                {
                    "id": py7zr.FILTER_LZMA2,
                    "preset": 9 | lzma.PRESET_EXTREME,
                }
            ]
            with py7zr.SevenZipFile(archive_path, mode="w", filters=filters) as archive:
                archive.write(source_path, arcname=source_path.name)
        except BaseException as exc:  # pragma: no cover - propagated to caller
            failure.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    while not done.wait(0.2):
        archive_size = archive_path.stat().st_size if archive_path.exists() else 0
        percent = min(95, int((archive_size / source_size) * 95))
        if progress_callback:
            progress_callback(percent, 100, f"Compressing to 7z... {percent}%")
        time_module.sleep(0.05)

    thread.join()
    if failure:
        raise ExportError(f"7z compression failed: {failure[0]}")
    if progress_callback:
        progress_callback(100, 100, f"Compression finished: {archive_path.name}")
    return archive_path


class Checklist(ttk.LabelFrame):
    def __init__(self, master: Any, *, title: str) -> None:
        super().__init__(master, text=title, padding=8)
        self._variables: dict[str, tk.BooleanVar] = {}
        self._labels: dict[str, str] = {}

        canvas = tk.Canvas(self, highlightthickness=0, height=180)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        self.content = ttk.Frame(canvas)

        self.content.bind(
            "<Configure>",
            lambda event: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.create_window((0, 0), window=self.content, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

    def set_items(self, items: Sequence[tuple[str, str]], *, default_checked: bool = True) -> None:
        for child in self.content.winfo_children():
            child.destroy()
        self._variables.clear()
        self._labels.clear()

        for row, (key, label) in enumerate(items):
            variable = tk.BooleanVar(value=default_checked)
            checkbox = ttk.Checkbutton(self.content, text=label, variable=variable)
            checkbox.grid(row=row, column=0, sticky="w", pady=1)
            self._variables[key] = variable
            self._labels[key] = label

    def selected_keys(self) -> list[str]:
        return [key for key, variable in self._variables.items() if variable.get()]

    def set_all(self, value: bool) -> None:
        for variable in self._variables.values():
            variable.set(value)


class AccessExportApp:
    def __init__(self) -> None:
        if tk is None or ttk is None:
            raise ExportError("Tkinter is not available in this Python installation.")

        self.root = tk.Tk()
        self.root.title("PostgreSQL to Tableau Export")
        self.root.geometry("980x760")
        self.root.minsize(860, 680)

        self.message_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.tables_by_label: dict[str, TableRef] = {}
        self.current_columns: list[str] = []
        self.current_indexes: list[TableIndex] = []
        self.worker: threading.Thread | None = None

        self.host_var = tk.StringVar()
        self.port_var = tk.StringVar(value="5432")
        self.db_var = tk.StringVar(value="postgres")
        self.user_var = tk.StringVar(value="postgres")
        self.password_var = tk.StringVar(value="postgres")
        self.table_var = tk.StringVar()
        self.format_var = tk.StringVar(value="accdb")
        self.compress_var = tk.BooleanVar(value=False)
        self.output_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Enter connection details, then click Connect.")

        self._load_settings()
        self._build_ui()
        self.format_var.trace_add("write", self._on_format_changed)
        self.table_var.trace_add("write", self._on_table_changed)
        self.root.after(150, self._drain_queue)

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=14)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(2, weight=1)

        connection_frame = ttk.LabelFrame(container, text="Step 1: Connection", padding=10)
        connection_frame.grid(row=0, column=0, sticky="ew")
        for index in range(8):
            connection_frame.columnconfigure(index, weight=1)

        ttk.Label(connection_frame, text="Host").grid(row=0, column=0, sticky="w")
        ttk.Entry(connection_frame, textvariable=self.host_var).grid(row=1, column=0, columnspan=2, sticky="ew", padx=(0, 8))
        ttk.Label(connection_frame, text="Port").grid(row=0, column=2, sticky="w")
        ttk.Entry(connection_frame, textvariable=self.port_var, width=8).grid(row=1, column=2, sticky="ew", padx=(0, 8))
        ttk.Label(connection_frame, text="Database").grid(row=0, column=3, sticky="w")
        ttk.Entry(connection_frame, textvariable=self.db_var).grid(row=1, column=3, sticky="ew", padx=(0, 8))
        ttk.Label(connection_frame, text="User").grid(row=0, column=4, sticky="w")
        ttk.Entry(connection_frame, textvariable=self.user_var).grid(row=1, column=4, sticky="ew", padx=(0, 8))
        ttk.Label(connection_frame, text="Password").grid(row=0, column=5, sticky="w")
        ttk.Entry(connection_frame, textvariable=self.password_var, show="*").grid(row=1, column=5, sticky="ew", padx=(0, 8))
        ttk.Button(connection_frame, text="Connect", command=self.connect).grid(row=1, column=6, sticky="ew")

        table_frame = ttk.LabelFrame(container, text="Step 2: Table", padding=10)
        table_frame.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        table_frame.columnconfigure(1, weight=1)

        ttk.Label(table_frame, text="Table").grid(row=0, column=0, sticky="w")
        self.table_combo = ttk.Combobox(table_frame, textvariable=self.table_var, state="readonly")
        self.table_combo.grid(row=0, column=1, sticky="ew", padx=(8, 8))
        ttk.Button(table_frame, text="Load Columns and Indexes", command=self.load_table_details).grid(row=0, column=2, sticky="ew")

        middle = ttk.Frame(container)
        middle.grid(row=2, column=0, sticky="nsew", pady=(12, 0))
        middle.columnconfigure(0, weight=1)
        middle.columnconfigure(1, weight=1)
        middle.rowconfigure(0, weight=1)

        left = ttk.Frame(middle)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)
        ttk.Label(left, text="Step 3: Columns").grid(row=0, column=0, sticky="w")
        self.columns_list = Checklist(left, title="Columns")
        self.columns_list.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        column_buttons = ttk.Frame(left)
        column_buttons.grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Button(column_buttons, text="All", command=lambda: self.columns_list.set_all(True)).pack(side="left")
        ttk.Button(column_buttons, text="None", command=lambda: self.columns_list.set_all(False)).pack(side="left", padx=(6, 0))

        right = ttk.Frame(middle)
        right.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)
        ttk.Label(right, text="Step 4: Indexes").grid(row=0, column=0, sticky="w")
        self.indexes_list = Checklist(right, title="Indexes")
        self.indexes_list.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        index_buttons = ttk.Frame(right)
        index_buttons.grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Button(index_buttons, text="All", command=lambda: self.indexes_list.set_all(True)).pack(side="left")
        ttk.Button(index_buttons, text="None", command=lambda: self.indexes_list.set_all(False)).pack(side="left", padx=(6, 0))

        export_frame = ttk.LabelFrame(container, text="Step 5: Export", padding=10)
        export_frame.grid(row=3, column=0, sticky="ew", pady=(12, 0))
        export_frame.columnconfigure(1, weight=1)

        ttk.Label(export_frame, text="Format").grid(row=0, column=0, sticky="w")
        format_buttons = ttk.Frame(export_frame)
        format_buttons.grid(row=0, column=1, sticky="w", padx=(8, 8))
        ttk.Radiobutton(format_buttons, text="ACCDB", value="accdb", variable=self.format_var).pack(side="left")
        ttk.Radiobutton(format_buttons, text="SAV", value="sav", variable=self.format_var).pack(side="left", padx=(12, 0))
        ttk.Checkbutton(
            export_frame,
            text="Compress to .7z after export (high compression)",
            variable=self.compress_var,
        ).grid(row=0, column=2, sticky="w")
        ttk.Label(export_frame, text="Output file").grid(row=1, column=0, sticky="w")
        ttk.Entry(export_frame, textvariable=self.output_var).grid(row=1, column=1, sticky="ew", padx=(8, 8))
        ttk.Button(export_frame, text="Browse", command=self.browse_output).grid(row=1, column=2, sticky="ew")

        self.progress = ttk.Progressbar(export_frame, mode="determinate", maximum=100)
        self.progress.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        ttk.Label(export_frame, textvariable=self.status_var).grid(row=3, column=0, columnspan=3, sticky="w", pady=(8, 0))
        ttk.Button(export_frame, text="Export", command=self.export).grid(row=4, column=2, sticky="e", pady=(10, 0))

    def run(self) -> None:
        self.root.mainloop()

    def _load_settings(self) -> None:
        saved = load_saved_settings()
        if saved.get("host"):
            self.host_var.set(saved["host"])
        if saved.get("port"):
            self.port_var.set(saved["port"])
        if saved.get("database"):
            self.db_var.set(saved["database"])
        if saved.get("username"):
            self.user_var.set(saved["username"])
        if saved.get("format") in {"accdb", "sav"}:
            self.format_var.set(saved["format"])
        if str(saved.get("compress", "")).lower() in {"true", "1", "yes"}:
            self.compress_var.set(True)

    def _connection_kwargs(self) -> dict[str, Any]:
        try:
            port = int(self.port_var.get().strip() or "5432")
        except ValueError as exc:
            raise ExportError("Port must be a whole number.") from exc
        return {
            "host": self.host_var.get().strip(),
            "port": port,
            "database": self.db_var.get().strip() or "postgres",
            "username": self.user_var.get().strip() or "postgres",
            "password": self.password_var.get(),
        }

    def browse_output(self) -> None:
        if filedialog is None:
            return
        output_format = self.format_var.get()
        chosen = filedialog.asksaveasfilename(
            defaultextension=f".{output_format}",
            filetypes=[
                ("Access Database", "*.accdb"),
                ("SPSS SAV", "*.sav"),
            ],
            initialfile=self.output_var.get() or make_timestamped_output_name("export", output_format),
        )
        if chosen:
            self.output_var.set(chosen)

    def connect(self) -> None:
        self.status_var.set("Connecting to PostgreSQL...")
        self.progress.configure(value=0)
        self._start_worker(self._connect_worker)

    def load_table_details(self) -> None:
        if not self.table_var.get():
            self._show_error("Choose a table first.")
            return
        self.status_var.set("Loading columns and indexes...")
        self.progress.configure(value=0)
        self._start_worker(self._load_table_worker)

    def export(self) -> None:
        label = self.table_var.get()
        table = self.tables_by_label.get(label)
        if table is None:
            self._show_error("Choose and load a table first.")
            return
        selected_columns = self.columns_list.selected_keys()
        if not selected_columns:
            self._show_error("Select at least one column.")
            return
        output_text = self.output_var.get().strip()
        if not output_text:
            self.output_var.set(str(Path.cwd() / make_timestamped_output_name(table.name, self.format_var.get())))
            output_text = self.output_var.get()
        elif not Path(output_text).suffix:
            self.output_var.set(str(Path(output_text).with_suffix(f".{self.format_var.get()}")))
        self.status_var.set("Export in progress...")
        self.progress.configure(value=0)
        self._start_worker(self._export_worker)

    def _on_format_changed(self, *_args: Any) -> None:
        current = self.output_var.get().strip()
        if not current:
            return
        path = Path(current)
        if path.suffix.casefold() in {".accdb", ".sav"}:
            self.output_var.set(str(path.with_suffix(f".{self.format_var.get()}")))

    def _on_table_changed(self, *_args: Any) -> None:
        label = self.table_var.get().strip()
        table = self.tables_by_label.get(label)
        if table is None:
            return

        current = self.output_var.get().strip()
        if current and not self._looks_like_generated_output(Path(current)):
            return

        base_dir = Path(current).expanduser().parent if current else Path.cwd()
        self.output_var.set(
            str(base_dir / make_timestamped_output_name(table.name, self.format_var.get()))
        )

    def _looks_like_generated_output(self, path: Path) -> bool:
        suffix = path.suffix.casefold()
        if suffix not in {".accdb", ".sav"}:
            return False
        return bool(re.fullmatch(r".+_\d{8}_\d{6}", path.stem))

    def _start_worker(self, target: Callable[[], None]) -> None:
        if self.worker and self.worker.is_alive():
            self._show_error("Another operation is already running.")
            return
        self.worker = threading.Thread(target=target, daemon=True)
        self.worker.start()

    def _connect_worker(self) -> None:
        try:
            engine = build_engine(**self._connection_kwargs())
            try:
                tables = list_user_tables(engine)
            finally:
                engine.dispose()
            if not tables:
                raise ExportError("No user tables were found.")
            self._save_settings()
            self.message_queue.put(("tables", tables))
        except Exception as exc:
            self.message_queue.put(("error", str(exc)))

    def _load_table_worker(self) -> None:
        try:
            table = self.tables_by_label[self.table_var.get()]
            engine = build_engine(**self._connection_kwargs())
            try:
                columns, indexes = load_table_metadata(engine, table)
            finally:
                engine.dispose()
            self.message_queue.put(("metadata", (table, columns, indexes)))
        except Exception as exc:
            self.message_queue.put(("error", str(exc)))

    def _export_worker(self) -> None:
        try:
            table = self.tables_by_label[self.table_var.get()]
            selected_column_names = self.columns_list.selected_keys()
            selected_index_names = set(self.indexes_list.selected_keys())
            selected_indexes = [item for item in self.current_indexes if item.name in selected_index_names]

            output_path = Path(self.output_var.get().strip()).expanduser().resolve()
            output_format = output_path.suffix.casefold().lstrip(".") or self.format_var.get()
            archive_path: Path | None = None
            engine = build_engine(**self._connection_kwargs())
            try:
                if output_format == "sav":
                    exported = export_table_to_sav(
                        engine=engine,
                        table=table,
                        selected_columns=selected_column_names,
                        output_path=output_path,
                        chunk_size=5000,
                        progress_callback=self._queue_progress,
                    )
                else:
                    exported = export_table_to_access(
                        engine=engine,
                        table=table,
                        selected_columns=selected_column_names,
                        selected_indexes=selected_indexes,
                        output_path=output_path,
                        chunk_size=5000,
                        progress_callback=self._queue_progress,
                    )
            finally:
                engine.dispose()
            if self.compress_var.get():
                archive_path = compress_with_py7zr(output_path, progress_callback=self._queue_progress)
            self._save_settings()
            self.message_queue.put(("done", (exported, output_path, archive_path)))
        except Exception as exc:
            details = traceback.format_exc()
            self.message_queue.put(("error", f"{exc}\n\n{details}"))

    def _save_settings(self) -> None:
        try:
            save_settings(
                host=self.host_var.get().strip(),
                port=self.port_var.get().strip() or "5432",
                database=self.db_var.get().strip() or "postgres",
                username=self.user_var.get().strip() or "postgres",
                output_format=self.format_var.get(),
                compress_output=self.compress_var.get(),
            )
        except OSError:
            pass

    def _queue_progress(self, exported_rows: int, total_rows: int, status: str) -> None:
        self.message_queue.put(("progress", (exported_rows, total_rows, status)))

    def _drain_queue(self) -> None:
        try:
            while True:
                kind, payload = self.message_queue.get_nowait()
                if kind == "tables":
                    tables: list[TableRef] = payload
                    labels = [item.qualified_name for item in tables]
                    self.tables_by_label = dict(zip(labels, tables))
                    self.table_combo["values"] = labels
                    if labels:
                        self.table_combo.current(0)
                    self.status_var.set(f"Connected. Found {len(labels)} table(s).")
                elif kind == "metadata":
                    table, columns, indexes = payload
                    self.current_columns = columns
                    self.current_indexes = indexes
                    self.columns_list.set_items([(column, column) for column in columns], default_checked=True)
                    self.indexes_list.set_items([(index.name, index.label) for index in indexes], default_checked=True)
                    if not self.output_var.get().strip():
                        self.output_var.set(str(Path.cwd() / make_timestamped_output_name(table.name, self.format_var.get())))
                    self.status_var.set(
                        f"Loaded {len(columns)} column(s) and {len(indexes)} index(es) for {table.qualified_name}."
                    )
                elif kind == "progress":
                    exported_rows, total_rows, status = payload
                    percent = 0 if total_rows <= 0 else min(100, (exported_rows / total_rows) * 100)
                    self.progress.configure(value=percent)
                    self.status_var.set(status)
                elif kind == "done":
                    exported, output_path, archive_path = payload
                    self.progress.configure(value=100)
                    message = f"Finished export: {exported:,} rows -> {output_path}"
                    if archive_path is not None:
                        message += f" | 7z: {archive_path}"
                    self.status_var.set(message)
                    if messagebox is not None:
                        dialog_text = f"Exported {exported:,} rows to:\n{output_path}"
                        if archive_path is not None:
                            dialog_text += f"\n\nCompressed archive:\n{archive_path}"
                        messagebox.showinfo("Export complete", dialog_text)
                elif kind == "error":
                    self.status_var.set("Operation failed.")
                    self._show_error(str(payload))
        except queue.Empty:
            pass
        finally:
            self.root.after(150, self._drain_queue)

    def _show_error(self, message: str) -> None:
        if messagebox is not None:
            messagebox.showerror("Error", message)


def parse_table_name(raw: str) -> TableRef:
    parts = [part for part in raw.split(".") if part]
    if len(parts) == 1:
        return TableRef(schema="public", name=parts[0])
    if len(parts) == 2:
        return TableRef(schema=parts[0], name=parts[1])
    raise ExportError(f"Invalid table name: {raw!r}. Use table or schema.table.")


def headless_export(args: argparse.Namespace) -> int:
    table = parse_table_name(args.table)
    output_format = args.format or "accdb"
    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else Path.cwd() / make_timestamped_output_name(table.name, output_format)
    )
    output_format = output_path.suffix.casefold().lstrip(".") or output_format
    engine = build_engine(
        host=args.host,
        port=args.port,
        database=args.database,
        username=args.username,
        password=args.password,
    )
    try:
        columns, indexes = load_table_metadata(engine, table)
        selected_columns = columns if args.all_columns or not args.columns else [item.strip() for item in args.columns.split(",") if item.strip()]
        if args.all_indexes or not args.indexes:
            selected_indexes = indexes
        else:
            wanted = {item.strip() for item in args.indexes.split(",") if item.strip()}
            selected_indexes = [item for item in indexes if item.name in wanted]

        def log_progress(exported_rows: int, total_rows: int, status: str) -> None:
            percent = 0 if total_rows <= 0 else (exported_rows / total_rows) * 100
            print(f"{percent:6.2f}%  {status}", flush=True)

        if output_format == "sav":
            exported = export_table_to_sav(
                engine=engine,
                table=table,
                selected_columns=selected_columns,
                output_path=output_path,
                chunk_size=args.chunk_size,
                progress_callback=log_progress,
            )
        else:
            exported = export_table_to_access(
                engine=engine,
                table=table,
                selected_columns=selected_columns,
                selected_indexes=selected_indexes,
                output_path=output_path,
                chunk_size=args.chunk_size,
                progress_callback=log_progress,
            )
        print(f"Exported {exported:,} rows to {output_path}")
        return 0
    finally:
        engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interactive PostgreSQL to Tableau exporter.")
    parser.add_argument("--headless", action="store_true", help="Run without the GUI.")
    parser.add_argument("--host", help="PostgreSQL host.")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port. Default: 5432.")
    parser.add_argument("--database", default="postgres", help="PostgreSQL database. Default: postgres.")
    parser.add_argument("--username", default="postgres", help="PostgreSQL username. Default: postgres.")
    parser.add_argument("--password", default="postgres", help="PostgreSQL password. Default: postgres.")
    parser.add_argument("--table", help="Table to export. Use table or schema.table.")
    parser.add_argument("--output", help="Output .accdb or .sav path. Defaults to a timestamped filename.")
    parser.add_argument("--format", choices=("accdb", "sav"), help="Default output format when --output is omitted.")
    parser.add_argument("--columns", help="Comma-separated list of columns to export.")
    parser.add_argument("--indexes", help="Comma-separated list of PostgreSQL index names to create in Access.")
    parser.add_argument("--all-columns", action="store_true", help="Export all columns.")
    parser.add_argument("--all-indexes", action="store_true", help="Create all discovered indexes.")
    parser.add_argument("--chunk-size", type=int, default=5000, help="Rows per chunk. Default: 5000.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.headless:
            if not args.host or not args.table:
                raise ExportError("--headless requires --host and --table.")
            if args.chunk_size <= 0:
                raise ExportError("--chunk-size must be greater than zero.")
            return headless_export(args)

        app = AccessExportApp()
        app.run()
        return 0
    except ExportError as exc:
        print(f"ERROR: {exc}")
        return 2
    except KeyboardInterrupt:
        print("\nCancelled.")
        return 130
    except Exception as exc:
        print(f"UNEXPECTED ERROR: {type(exc).__name__}: {exc}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
