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

## PostgreSQL Data Model

`stg_healthcare_claims`: Staging landing zone for cleaned rows before upsert
`fact_claims`: Curated analytics table
`rejected_claims`: for invalid rows
`pipeline_audit`: Per-run counts, status, and errors

- Schema script: [`sql/create_tables.sql`](sql/create_tables.sql)

### Start the database

```bash
docker compose up -d
# Tables are created automatically on first start via create_tables.sql
```

## Synthetic Data

Generate three claim CSV files (10k–20k total rows) with mostly valid data plus controlled defects for validation testing:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python generate_claims.py --records 20000 --files 3
```

Output lands in `data/incoming/healthcare_claims_part_01.csv` … `_03.csv`. Defect types include missing fields, bad dates, negative amounts, invalid statuses, missing denial reasons, and business-key duplicates. All IDs are synthetic — no real PHI.

## Ingestion

Read all CSVs from `data/incoming`, skip empty or bad-schema files, and combine rows with lineage columns:

```bash
source .venv/bin/activate
python "src/ingestion.py"
```

Adds `source_filename`, `pipeline_run_id`, and `ingestion_timestamp` to every row. Config comes from `config/settings.yaml` via `src/config.py`.
