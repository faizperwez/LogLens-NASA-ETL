# LogLens — NASA HTTP Traffic ETL

A beginner-sized data engineering project that ingests **real historical web-server access logs** from the public NASA-HTTP dataset mirrored on Kaggle, parses the raw log lines, validates records with Python/Pandas, writes clean and rejected outputs, loads valid events into PostgreSQL, and provides SQL queries for traffic analysis.

**Scope:** a local ETL prototype, not production monitoring. The full dataset is not bundled in this repository; use the included sample to test the code, then download the real dataset from Kaggle.

## Why this is more useful than a toy sales CSV

- The source is a raw access-log text format, not a ready-made analysis table.
- The ETL has to parse each line, split an HTTP request into method/path/protocol, and validate timestamps, status codes, and response bytes.
- Bad rows are preserved in a reject file with a reason.
- The pipeline streams the input in batches instead of loading the entire log into memory.
- SQL queries answer real operational questions: which resources receive the most requests, when traffic peaks, what fraction of requests return 4xx/5xx codes, and how much data is transferred.
- PostgreSQL uses a stable `source_id + source_line` record identity so repeated loads don't insert the same source line twice.

The NASA dataset is not unique in itself—it's a known web-log dataset. The value of this project should be your implementation and your understanding of parsing, validation, idempotent loading, and the limits of the data.

## Data source and attribution

- **Kaggle dataset:** https://www.kaggle.com/datasets/adchatakora/nasa-http-access-logs
- **Original dataset documentation:** https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

The original Internet Traffic Archive describes records with a requesting host, timestamp, quoted HTTP request, response code, and response bytes. It notes that the traces may be freely redistributed, and asks users not to analyse them beyond general traffic patterns. For that reason, this project keeps the host field for source fidelity but does **not** include host-level leaderboards or individual-host profiling in its SQL examples.

The real NASA data records request time, status, and response bytes; it does **not** provide per-request response latency. Therefore this project analyses response bytes and traffic/errors—not p95 latency. Do not claim it measures response speed.

## Project layout

```text
LogLens_NASA_ETL/
├── loglens/
│   ├── etl.py                         # Parse, validate, stream, write outputs, load PostgreSQL
│   └── __init__.py
├── data/
│   ├── sample/nasa_access_sample.log   # Small illustrative sample, not Kaggle data
│   └── raw/                            # Put downloaded Kaggle dataset here (ignored by Git)
├── output/                             # Clean and rejected CSV outputs (ignored by Git)
├── scripts/
│   └── download_kaggle_dataset.py      # Optional Kaggle CLI downloader
├── sql/
│   ├── schema.sql                      # PostgreSQL table and indexes
│   └── analytics.sql                   # Traffic, status, errors, resource and bytes queries
├── tests/test_etl.py
├── .env.example
└── requirements.txt
```

## 1. Set up in VS Code on macOS

Open this folder in VS Code, then choose **Terminal → New Terminal**.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

In VS Code, select the project's interpreter using `Cmd+Shift+P` → **Python: Select Interpreter** → `.venv/bin/python`.

## 2. Run the bundled sample first

The sample is a small hand-written file using the public dataset's log format. It is included so the project runs before you download anything; it is **not** real NASA traffic.

```bash
python -m loglens.etl --input data/sample/nasa_access_sample.log --source-id sample --dry-run
```

Expected behaviour: the command reports some accepted records and some rejected records, then writes:

- `output/cleaned_nasa_logs.csv`
- `output/rejected_nasa_logs.csv`

The rejected file includes the source line and a reason such as `malformed_log_line`, `invalid_timestamp`, `invalid_status_code`, or `invalid_bytes_sent`.

## 3. Download the real Kaggle dataset

### Option A — browser (simplest)

1. Open https://www.kaggle.com/datasets/adchatakora/nasa-http-access-logs.
2. Sign in to Kaggle if prompted and download the dataset.
3. Extract the archive. Find the NASA access-log text file (usually `.log`, `.txt`, or a compressed `.gz` file).
4. Put the extracted text file at `data/raw/nasa_access.log`.

If the downloaded file has a different name, either rename it to `nasa_access.log` or pass its path with `--input`.

### Option B — Kaggle CLI

The CLI requires a Kaggle account and API token. Install it inside the active virtual environment:

```bash
python -m pip install kaggle
```

Configure your Kaggle API credentials according to Kaggle's current API instructions. Keep the token private; never commit it or paste it into a chat. Then run:

```bash
python scripts/download_kaggle_dataset.py
```

The script runs the Kaggle dataset download command and normalizes the discovered log file to `data/raw/nasa_access.log`. If it cannot find the downloaded file, use Option A and place the extracted text file there manually.

## 4. Run the real dataset in dry-run mode

Start with 100,000 lines so you can confirm parsing and output shape quickly:

```bash
python -m loglens.etl --input data/raw/nasa_access.log --source-id nasa_aug95 --dry-run --limit 100000
```

Inspect both CSVs in VS Code. If the accepted/rejected split looks reasonable, run the full dataset without `--limit`:

```bash
python -m loglens.etl --input data/raw/nasa_access.log --source-id nasa_aug95 --dry-run
```

The script processes the file in chunks (5,000 lines by default) to limit memory use. You can change that with `--chunk-size 5000`.

## 5. Load into PostgreSQL

1. Make sure PostgreSQL is running.
2. Create a database named `loglens` in DBeaver.
3. Copy `.env.example` to `.env` at the project root.
4. Edit `DATABASE_URL` with your local PostgreSQL username/password. Example format:

   ```text
   postgresql+psycopg://postgres:YOUR_PASSWORD@localhost:5432/loglens
   ```

5. First try a limited load:

   ```bash
   python -m loglens.etl --input data/raw/nasa_access.log --source-id nasa_aug95 --limit 100000
   ```

6. Once confirmed, load the full dataset by removing `--limit`.

Do not use `--dry-run` when you want to load into PostgreSQL. The script creates the table and indexes inside the existing database; it does not create the database itself. The record ID includes the stable source ID and source line number. Use the **same** `--source-id` for repeat runs of the same file so PostgreSQL can skip already-loaded rows. Use a different source ID for a different source file.

## 6. Analyse in DBeaver

Open `sql/analytics.sql` and run the queries one at a time. They cover:

1. HTTP status-code distribution.
2. Request counts and 4xx/5xx rates by resource.
3. Traffic volume and transferred bytes by hour.
4. Most requested resources.
5. Daily request/error/transfer totals.
6. A small, non-host-specific error sample.

The status-code query treats all `4xx` and `5xx` responses as errors for that report. That is a project definition, not a statement that every 4xx is a server fault.

## 7. Run tests

```bash
pytest -q
```

Tests cover log parsing, malformed rows, query-string normalization, missing byte counts, and validation of bad statuses/negative bytes. The tests do not require PostgreSQL and do not connect to Kaggle.

## Data-quality and design decisions

- Timestamp strings are parsed using the timestamp format in NASA Common Log Format and stored as timezone-aware timestamps in PostgreSQL.
- A response byte count of `-` is represented as SQL `NULL`, because the size is unavailable rather than zero.
- Non-parsable timestamps, status codes outside 100–599, and invalid/negative byte counts are rejected with explicit reasons.
- Lines with malformed structure or malformed HTTP request fields are rejected and retained in the rejected output.
- Repeated-looking requests are not deduplicated: two identical requests on different source lines can be two real events.
- `record_id` is based on source ID and physical line number; this makes reruns idempotent for the same unchanged file. If the source file is reordered or changed, use a new source ID.
- Query parameters are removed from `url_path` for resource aggregation so unique query strings do not create excessive groups. If query parameters are analytically important, store them in a separate field instead.
- Hostnames/IP-like identifiers are stored but not used for individual-host analysis, consistent with the source archive's caution.

## Known limitations

- This is a local prototype. It has no scheduler, dashboard, alerting, cloud deployment, or distributed compute.
- It records status codes and response bytes, not latency; it cannot identify slow requests.
- A source line is the idempotency key, so source file identity and line ordering must stay stable across reruns.
- The full dataset should not be committed to Git. The raw-data folder and generated outputs are ignored by `.gitignore`.
- The dataset is historical (1995); it is useful for learning log parsing and ETL, not for making claims about modern web traffic.

## Interview explanation (adapt this after you run it yourself)

“I built a batch ETL pipeline for NASA HTTP access logs. It reads raw Common Log Format text, parses the HTTP request fields, uses Pandas to normalize timestamps and validate status/byte values, and separates invalid lines into a reject file with reasons. It streams the input in chunks, loads accepted records into PostgreSQL, and uses a stable source ID plus line number to make reruns idempotent. I use SQL to analyse request volume, HTTP error rates, popular resources, and bytes transferred over time. The dataset is historical and does not contain response latency, so I do not claim to measure p95 latency. This is a local portfolio prototype, not a production monitoring platform.”
