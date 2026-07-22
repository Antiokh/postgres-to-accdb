# PostgreSQL → Tableau Public

`pg_to_tableau.py` exports the result of a PostgreSQL table, view, set-returning
function, inline SQL query, or SQL file.

## Which output to use

### `.accdb` — recommended on Windows

Use this when:

- the result can be large;
- you want streaming export instead of holding the whole result in RAM;
- you want several exported tables in one file;
- you want primary keys or indexes.

Requirements:

- 64-bit Python should normally be paired with the 64-bit Microsoft Access
  Database Engine;
- Tableau Public can open the resulting file through **Microsoft Access**.

### `.sav` — simple portable binary file

Use this when:

- you need a single flat result;
- it fits comfortably in RAM;
- you want a file that Tableau Public opens through **Statistical File**;
- you do not need indexes.

The SAV writer must assemble the whole DataFrame in memory before saving.

## Installation

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Set the connection once:

```powershell
$env:PG_DSN = "postgresql+psycopg://postgres:PASSWORD@localhost:5432/database"
```

Use URL escaping for special characters in the password.

## Export a table or view to Access

```powershell
python pg_to_tableau.py `
  --relation public.traffic_accidents `
  --output traffic.accdb `
  --target-table traffic_accidents `
  --primary-key accident_id `
  --index idx_year=year `
  --index idx_municipality_year=municipality_formatted,year
```

## Export a view to SAV

```powershell
python pg_to_tableau.py `
  --relation public.tableau_accidents `
  --output tableau_accidents.sav
```

## Export a PostgreSQL function

Function parameters are positional. Values are parsed as JSON where possible.

```powershell
python pg_to_tableau.py `
  --function public.get_accidents `
  --arg 2024 `
  --arg '"Beograd"' `
  --output accidents_2024.accdb
```

`--arg 2024` is a number. `--arg '"Beograd"'` is an explicit JSON string.
A bare `--arg Beograd` also remains a string.

## Export inline SQL with named parameters

```powershell
python pg_to_tableau.py `
  --sql "SELECT * FROM public.traffic_accidents WHERE year = :year" `
  --params '{"year": 2024}' `
  --output accidents_2024.accdb
```

## Export a SQL file

```powershell
python pg_to_tableau.py `
  --sql-file query.sql `
  --params '{"year": 2024}' `
  --output result.sav
```

## Append another result to an existing Access table

```powershell
python pg_to_tableau.py `
  --sql-file next_period.sql `
  --output traffic.accdb `
  --target-table traffic_accidents `
  --if-exists append
```

The source column names and order must match the existing Access table.
Indexes are created only during the initial `replace` or `fail` run.

## Index syntax

```text
--primary-key accident_id
--primary-key municipality,year

--index idx_year=year
--index idx_municipality_year=municipality,year

--unique-index uid_external=external_id
```

Indexes are created **after** the bulk load, because maintaining them row by row
would slow the export.

## Important type behavior

- PostgreSQL `timestamptz` values are converted to naive UTC timestamps in
  Access because Access has no timezone-aware timestamp type.
- JSON, arrays, UUIDs, ranges, enums, and unfamiliar custom values are exported
  as text when Access has no direct equivalent.
- Access field and table names are limited to 64 characters.
- For unusual PostgreSQL identifiers containing spaces or quoted dots, use
  `--sql` or `--sql-file` and alias the result columns.
- `.sav` variable names are normalized when SPSS naming rules require it; the
  original name is retained as the variable label.

## PostgreSQL-side indexes

Indexes in PostgreSQL still matter for the export query itself. Add them to
columns used in `WHERE`, `JOIN`, and sometimes `ORDER BY`. They are not copied
into Tableau or into SAV. Access indexes are separate objects created in the
generated `.accdb`.
