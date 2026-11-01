# LogLens — NASA HTTP Access Log ETL

LogLens is a Python and PostgreSQL ETL pipeline for processing historical NASA web-server access logs. It parses raw access-log lines, validates request fields, writes accepted and rejected records to CSV, and loads valid records into PostgreSQL for SQL analysis.

## Tech stack

- Python 3.10+
- Pandas
- PostgreSQL
- SQLAlchemy and Psycopg
- Pytest

## Pipeline

1. **Extract:** Read the raw log file incrementally in batches (5,000 lines by default).
2. **Transform:** Parse the Common Log Format-style entries, convert data types, and validate timestamps, request fields, status codes and response sizes.
3. **Separate invalid records:** Write rejected lines to a CSV with their source line number and rejection reason.
4. **Load:** Insert valid records into PostgreSQL when the pipeline is run without `--dry-run`.
5. **Analyse:** Run SQL queries for request volume, HTTP status distribution, error rates, popular resources and bytes transferred.

## Dataset

The project uses the [NASA HTTP Access Log dataset on Kaggle](https://www.kaggle.com/datasets/adchatakora/nasa-http-access-logs), based on the [NASA-HTTP contribution to the Internet Traffic Archive](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html).

The full dataset is not included in this repository. A small illustrative log file is included so the pipeline can be tested before downloading the original data.

The source contains request timestamps, HTTP request details, response status codes and response byte counts. It does **not** contain request duration, so response latency and p95 latency cannot be calculated from these records.

## Repository structure

```text
LogLens-NASA-ETL/
├── data/
│   ├── sample/nasa_access_sample.log
│   └── raw/                         # Downloaded source data (not tracked)
├── loglens/
│   ├── __init__.py
│   └── etl.py                       # Parsing, validation, batching and loading
├── output/                          # Generated CSV files (not tracked)
├── scripts/
│   └── download_kaggle_dataset.py
├── sql/
│   ├── schema.sql
│   └── analytics.sql
├── tests/
│   └── test_etl.py
├── .env.example
├── .gitignore
├── pytest.ini
└── requirements.txt
```

## Setup

The commands below use macOS and VS Code.

### 1. Clone the repository and install dependencies

```bash
git clone https://github.com/faizperwez/LogLens-NASA-ETL.git
cd LogLens-NASA-ETL
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

In VS Code, select `.venv/bin/python` using **Cmd+Shift+P → Python: Select Interpreter**.

### 2. Run the included sample

The sample is illustrative; it is not a capture of current NASA traffic.

```bash
python -m loglens.etl --input data/sample/nasa_access_sample.log --source-id sample --dry-run
```

This writes two files under `output/`:

- `cleaned_nasa_logs.csv` — records that passed parsing and validation.
- `rejected_nasa_logs.csv` — rejected lines and their reasons.

The `--dry-run` option processes the input and writes CSV files without connecting to PostgreSQL.

## Get the NASA dataset

### Browser download

1. Download the archive from the [Kaggle dataset page](https://www.kaggle.com/datasets/adchatakora/nasa-http-access-logs).
2. Extract the access-log text file.
3. Place it at `data/raw/nasa_access.log`.

If the file has a different name, rename it or pass its path with `--input`.

### Kaggle CLI (optional)

The CLI requires a Kaggle account and a configured Kaggle API token. Keep the token private and outside this repository.

```bash
python -m pip install kaggle
python scripts/download_kaggle_dataset.py
```

The downloader searches `data/raw/` for a `.log`, `.txt` or `.gz` file. If the archive contains multiple candidate files, inspect the selected file before processing it.

## Run the ETL pipeline

Start with a 100,000-line dry run and inspect both output CSVs:

```bash
python -m loglens.etl \
  --input data/raw/nasa_access.log \
  --source-id nasa_aug95 \
  --dry-run \
  --limit 100000
```

To process the full file without loading it into PostgreSQL, remove `--limit` and keep `--dry-run`:

```bash
python -m loglens.etl --input data/raw/nasa_access.log --source-id nasa_aug95 --dry-run
```

The default batch size is 5,000 lines. Change it with `--chunk-size` if needed.

## Load data into PostgreSQL

1. Start PostgreSQL and create a database named `loglens` using DBeaver or another PostgreSQL client. The ETL script creates the table and indexes; it does not create the database.
2. Copy `.env.example` to `.env`:

   ```bash
   cp .env.example .env
   ```

3. Set `DATABASE_URL` in `.env`. Example with a password:

   ```env
   DATABASE_URL=postgresql+psycopg://postgres:YOUR_PASSWORD@localhost:5432/loglens
   ```

   Example for a local PostgreSQL account that does not require a password:

   ```env
   DATABASE_URL=postgresql+psycopg://postgres@localhost:5432/loglens
   ```

4. Run a limited load by omitting `--dry-run`:

   ```bash
   python -m loglens.etl --input data/raw/nasa_access.log --source-id nasa_aug95 --limit 100000
   ```

5. After verifying the rows in PostgreSQL, remove `--limit` to load the full file.

Keep `.env` private; it is excluded by `.gitignore`. Use the same `--source-id` when rerunning the same unchanged file. The pipeline builds `record_id` from the source ID and physical source line number, and PostgreSQL skips conflicting record IDs. Use a new source ID if the source file changes or is reordered.

## PostgreSQL table

The pipeline creates `public.web_requests` with the following columns:

| Column | Type | Description |
|---|---|---|
| `record_id` | `VARCHAR(200)` | Primary key built from source ID and source line |
| `source_id` | `VARCHAR(100)` | Identifier for the input file |
| `source_line` | `BIGINT` | Original line number in the source file |
| `event_timestamp` | `TIMESTAMPTZ` | Request timestamp stored in UTC |
| `host` | `TEXT` | Host value from the log |
| `http_method` | `VARCHAR(32)` | HTTP method, such as GET or POST |
| `url_path` | `TEXT` | Requested resource path |
| `protocol` | `VARCHAR(32)` | HTTP protocol, when present |
| `status_code` | `INTEGER` | HTTP status code (100–599) |
| `bytes_sent` | `BIGINT` | Response size; `NULL` when unavailable in the source |

The table has a uniqueness constraint on `(source_id, source_line)` and indexes on timestamp, URL path and status code.

## SQL analysis

Open `sql/analytics.sql` in DBeaver and run the queries individually. They cover:

- HTTP status-code distribution
- Request counts and 4xx/5xx error rates by resource
- Hourly request volume and transferred bytes
- Most requested resources
- Daily request, error and transfer totals
- A limited sample of error records

The endpoint error-rate query groups both 4xx and 5xx responses as errors. 4xx responses are client errors; they are not server errors.

## Data validation

- Parses the source timestamp format and stores timezone-aware timestamps in UTC.
- Requires status codes to be integers between 100 and 599.
- Requires response byte counts to be non-negative integers. A source value of `-` is stored as `NULL` because the size is unavailable.
- Rejects malformed log lines and invalid request fields, timestamps, status codes or byte counts, with a reason recorded in the rejected CSV.
- Removes query strings from `url_path` for resource-level aggregation.
- Retains identical-looking requests on different source lines because they may represent separate events.

## Tests

Run the tests with:

```bash
pytest -q
```

The tests cover valid and malformed log parsing, query-string removal, unavailable response-byte values, invalid status codes and negative byte counts. They do not require PostgreSQL or Kaggle credentials.

## Limitations

- This is a local batch ETL project; it has no scheduler, dashboard, alerting, cloud deployment or distributed processing.
- The source logs are historical (1995) and should not be used to describe current web traffic.
- The data does not include request duration, so latency metrics cannot be calculated.
- Idempotent loading depends on a stable source ID and source-line order. Use a new source ID if the input file changes or is reordered.
- The source archive advises analysis at the level of general traffic patterns. The example SQL does not rank or profile individual hosts.
- Some byte-transfer summaries use `COALESCE(bytes_sent, 0)`, meaning unavailable byte counts contribute zero to those sums.

## References

- [NASA HTTP Access Logs — Kaggle](https://www.kaggle.com/datasets/adchatakora/nasa-http-access-logs)
- [NASA-HTTP — Internet Traffic Archive](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html)
