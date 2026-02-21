# Architecture Decisions

## ADR-001: Medallion Architecture (Bronze → Silver → Gold)

**Decision:** Three-layer data architecture with clear separation of concerns.

- **Bronze** — Raw, immutable, exactly as received from sources. Enables reprocessing.
- **Silver** — Cleansed: deduplicated, type-cast, standardized. Single source of truth.
- **Gold** — Business-ready: star schema optimized for analytics and BI tools.

**Why:** Industry standard (Netflix, Airbnb, Uber). Clear lineage, reprocessable from raw data, separates data engineering from analytics.

## ADR-002: BigQuery Views for Silver Layer

**Decision:** Silver models are views, not tables.

**Why:** $0 storage cost, always reflect current bronze data, act as a contract between raw and gold layers. If source schemas change, only the view SQL needs updating.

## ADR-003: Star Schema for Gold Layer (Kimball)

**Decision:** Dimensional model with fact tables (measures) and dimension tables (context).

**Why:** Optimized for aggregation queries. BI tools perform best against star schemas. Predictable join patterns. Industry standard for analytics warehouses.

## ADR-004: Partition by Date + Cluster by Filter Columns

**Decision:** All fact tables partitioned by date (DAY), clustered by most-filtered columns.

**Why:** BigQuery charges per bytes scanned. Partitioning reduces scan by 10-100x for date-filtered queries. Clustering further reduces for store/product filters.

## ADR-005: Pre-Aggregated Daily Sales Table

**Decision:** `fct_daily_sales` pre-computes metrics that dashboards need.

**Why:** Avoids scanning 200K+ rows for every trend chart. Dashboard queries hit 40K rows instead, resulting in faster load times and lower cost.

## ADR-006: Customer Segmentation in Dimension

**Decision:** RFM-based segments computed in `dim_customer` rather than at query time.

**Why:** Consistent segmentation across all reports. No risk of different dashboards using different segment logic. Single place to update thresholds.
