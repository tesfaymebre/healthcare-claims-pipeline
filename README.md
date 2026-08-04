# Healthcare Claims Data Engineering Pipeline

End-to-end batch pipeline that ingests synthetic healthcare claims CSVs, validates business rules, transforms records, preserves rejects, and loads curated data into PostgreSQL for analytics.

## Business Problem

Claims, operations, finance, and data-quality teams need a trusted PostgreSQL dataset instead of inconsistent raw CSVs. This pipeline delivers reliable status counts, denial insights, payment totals, and visibility into invalid or duplicate claims.

## Architecture Overview

```
              Raw CSV Files

                     |

              Python Ingestion -> add run_id, source_filename, ingestion_timestamp

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