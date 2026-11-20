# Healthcare Claims Data Engineering Pipeline

End-to-end batch pipeline that ingests synthetic healthcare claims CSVs, validates business rules, transforms records, preserves rejects, and loads curated data into PostgreSQL for analytics.

## Business Problem

Claims, operations, finance, and data-quality teams need a trusted PostgreSQL dataset instead of inconsistent raw CSVs. This pipeline delivers reliable status counts, denial insights, payment totals, and visibility into invalid or duplicate claims.

## Architecture Overview

```
              Raw CSV Files

                     |

              Python Ingestion -> add source_filename, pipeline_run_id, and loaded_at

                       |

Schema and Data-quality Validation

            /                 \
Valid Records                  Rejected Records
      |                                |

Transformation                 rejected_claims (PostgreSQL)

      |

PostgreSQL Staging Table

      |

Fact Claims -> upsert on claim_id

      |

 Analytics
```

The pipeline also supports SQL analytics, indexing, logging, auditing, and idempotent reruns.


## Project Structure

```text
healthcare-claims-pipeline/
│
├── config/
│   └── settings.yaml
│
├── data/
│   ├── incoming/
│   ├── processed/
│   ├── rejected/
│   └── archive/
│
├── sql/
│   ├── create_tables.sql
│   ├── indexes.sql
│   └── analytics_queries.sql
│
├── src/
│   ├── config.py
│   ├── database.py
│   ├── ingestion.py
│   ├── validation.py
│   ├── transformation.py
│   ├── loading.py
│   └── audit.py
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_validation.py
│   ├── test_transformation.py
│   └── test_pipeline.py
│
├── logs/
│   └── pipeline.log
│
├── .env
├── .env.example
└── README.md
```

## Pipeline Steps

### 1. Ingestion

The ingestion process:

* Searches the incoming directory for CSV files.
* Reads the files using Pandas.
* Combines records from multiple files.
* Preserves the source filename.
* Generates a pipeline run ID.
* Adds ingestion metadata.
* Checks the expected source schema.

### 2. Validation

The validation stage checks data quality before records enter the database.

Some of the checks include:

* Required fields
* Valid dates
* Submission date not earlier than service date
* Positive billed amount
* Non-negative approved and paid amounts
* Paid amount not exceeding approved amount
* Valid claim statuses
* Valid claim types
* Required denial reason for denied claims
* Possible duplicate detection

Invalid records are not discarded.

They are stored in `rejected_claims` with the rejection reason and source information.

### 3. Transformation

Valid records are prepared for database loading.

The transformation stage handles:

* Trimming text values
* Converting blank strings to `NULL`
* Converting dates
* Converting timestamps
* Converting monetary fields to numeric values
* Selecting the columns required by the staging table

Derived fields are calculated when loading into the fact table.

### 4. Staging

Validated and transformed records are first loaded into:

```text
stg_healthcare_claims
```

The staging table provides an isolated area between the source/processing layer and the production-style fact table.

This makes it easier to:

* Inspect data before final loading
* Debug problems
* Reprocess a batch
* Reconcile data
* Handle schema changes
* Prevent partially processed data from becoming production data

### 5. Fact Table

Clean claims are merged into:

```text
fact_claims
```

The pipeline uses:

```sql
INSERT ... ON CONFLICT (claim_id) DO UPDATE
```

This allows existing claims to be updated instead of inserting another copy.

The fact table also contains derived fields such as:

* `processing_days`
* `unpaid_amount`
* `is_approved`
* `is_denied`
* `high_value_flag`
* `late_submission_flag`
* `duplicate_flag`

### 6. Duplicate Detection

Possible duplicates are identified using the business key:

```text
patient_id
provider_id
service_date
procedure_code
billed_amount
```

The duplicate flag is calculated against the existing fact-table history, rather than only the current batch.

This is important because a new record can make an existing record a possible duplicate.

### 7. SQL Analytics

The project includes queries for:

* Claims count and monetary totals by status
* Approval and denial rates
* Top denial reasons
* Providers with the highest billed and paid amounts
* Average processing time by claim type
* Monthly claim volume and payment trends
* Potential duplicate claims
* High-value claims requiring review

The queries are available in:

```text
sql/analytics_queries.sql
```

### 8. Indexing and Performance

Indexes were selected based on actual query patterns and `EXPLAIN (ANALYZE, BUFFERS)` results.

The final indexes support:

* Claim status filtering
* Provider lookups
* Service-date queries
* Pipeline-run reconciliation
* Duplicate/business-key lookups

The index definitions are separated into:

```text
sql/indexes.sql
```

This keeps table definitions and performance-related decisions separate.

## Database Tables

The main PostgreSQL tables are:

### `stg_healthcare_claims`

Contains validated, transformed records before the fact-table merge.

### `fact_claims`

Contains the final analytical claim records and derived fields.

### `rejected_claims`

Stores records that failed validation together with their rejection reasons and source information.

### `pipeline_audit`

Stores information about pipeline executions, including:

* Pipeline run ID
* Start/end time
* Records received
* Records loaded
* Records rejected
* Duplicate counts
* Run status
* Error messages

## Configuration

Pipeline configuration is stored in:

```text
config/settings.yaml
```

This includes:

* Pipeline name and version
* Directory paths
* Validation thresholds
* Batch size
* Source schema
* Required fields
* Allowed statuses
* Duplicate key fields
* Logging configuration

Database credentials are kept separately in `.env`.

Example:

```text
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=healthcare_claims
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

Do not commit `.env` to Git.

## Running the Project

### 1. Start PostgreSQL

If using Docker Compose:

```bash
docker compose up -d
```

### 2. Activate the virtual environment

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Initialize the database

```bash
python src/database.py
```

This applies:

```text
sql/create_tables.sql
sql/indexes.sql
```

### 5. Place CSV files in the incoming directory

```text
data/incoming/
```

### 6. Run the pipeline

```bash
python src/loading.py
```

## Testing

The project uses pytest.

Run all tests with:

```bash
pytest -v
```

Current test coverage includes:

* Missing input files
* Invalid input schemas
* Valid records
* Missing required fields
* Invalid financial values
* Invalid dates
* Invalid payment amounts
* Text transformation
* Date transformation
* Amount transformation
* Transaction rollback
* Idempotent reruns

Current test result:

```text
12 passed
```

## Idempotency

The pipeline is designed so that processing the same data again does not create duplicate fact records.

For example, the first run can produce:

```text
inserted:   18,601
updated:         0
unchanged:       0
```

Running the same data again produces:

```text
inserted:        0
updated:         0
unchanged: 18,601
```

This allows the pipeline to be safely rerun.

## Example Processing Result

One test run processed:

```text
Records received:        20,820
Records valid:           18,621
Records rejected:         2,199

Rows staged:             18,621
Merge candidates:        18,601
Inserted:                18,601
Updated:                      0
Unchanged:                    0

Fact rows:               18,601
Possible duplicates:      1,406
High-value claims:            6
Late submissions:              0
```

## Logging and Auditing

Pipeline logs are written to:

```text
logs/pipeline.log
```

Pipeline execution information is also stored in PostgreSQL in:

```text
pipeline_audit
```

This makes it possible to trace a pipeline run using its `pipeline_run_id`.

## Design Decisions

A few important design decisions in this project are:

### Staging before the fact table

Staging provides isolation and gives the pipeline an opportunity to inspect, reconcile, and reprocess data before it becomes part of the final fact table.

### Rejected records are preserved

Invalid records are stored instead of being silently dropped. This makes debugging and data-quality investigation possible.

### Duplicate detection after the fact merge

Duplicate detection depends on historical data, so it is performed with visibility into the existing fact table.

### Configuration-driven thresholds

Business rules such as the high-value threshold are stored in configuration instead of being hardcoded in the pipeline.

### Indexes based on workload

Indexes were tested using `EXPLAIN (ANALYZE, BUFFERS)` instead of simply creating an index for every column.

## Future Improvements

Possible improvements for a production version include:

* Airflow or another workflow orchestrator
* More extensive automated test coverage
* CI/CD with GitHub Actions
* Incremental processing for very large datasets
* More detailed monitoring and alerting
* Schema migration management
* Data-quality reporting/dashboard
* Partitioning for a much larger fact table

## Author

Healthcare Claims Data Engineering Training Project.
