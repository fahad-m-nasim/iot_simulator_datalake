# Databricks IoT Lakehouse Pipeline

Modern data lakehouse implementation using **Lakeflow Declarative Pipelines** (SQL-only)
following Databricks best practices. All transformations from Bronze to Gold are implemented
in pure SQL using Streaming Tables and Materialized Views.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    IoT Lakehouse - Lakeflow Declarative Pipelines               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────┐    ┌───────────────────┐    ┌───────────────────────────────┐ │
│  │     S3      │───▶│   STREAMING       │───▶│      MATERIALIZED VIEWS       │ │
│  │ (Raw Data)  │    │   TABLES          │    │                               │ │
│  │             │    │   (Bronze)        │    │  Silver          Gold         │ │
│  └─────────────┘    │                   │    │  ┌─────────┐    ┌──────────┐  │ │
│                     │  • iot_events     │    │  │ stg_*   │───▶│ dim_*    │  │ │
│  IoT Events ───────▶│  • cdc_customers  │───▶│  │ models  │    │ fct_*    │  │ │
│  CDC Data ─────────▶│  • cdc_products   │    │  └─────────┘    │ agg_*    │  │ │
│                     │  • cdc_orders     │    │                 └──────────┘  │ │
│                     │  • cdc_devices    │    │                               │ │
│                     │  • cdc_locations  │    │  All SQL-based transformations│ │
│                     │  • cdc_alerts     │    │  with data quality expectations│ │
│                     │  • cdc_thresholds │    │                               │ │
│                     └───────────────────┘    └───────────────────────────────┘ │
│                                                                                  │
│  Technology Stack:                                                               │
│  • Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables)            │
│  • Streaming Tables with Auto Loader (Bronze)                                   │
│  • Materialized Views with CDC processing (Silver/Gold)                         │
│  • Unity Catalog for governance                                                 │
│  • Pure SQL - No Python notebooks required                                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
iot_simulator_datalake/
├── databricks.yml              # Main DAB configuration
├── resources/
│   ├── clusters.yml            # Cluster definitions
│   ├── pipelines.yml           # Lakeflow pipeline configuration
│   └── jobs.yml                # Job definitions
├── pipelines/                  # SQL Declarative Pipelines
│   ├── bronze/                 # Streaming Tables (Ingestion)
│   │   ├── ingest_iot_events.sql
│   │   ├── ingest_cdc_customers.sql
│   │   ├── ingest_cdc_products.sql
│   │   ├── ingest_cdc_orders.sql
│   │   ├── ingest_cdc_devices.sql
│   │   ├── ingest_cdc_locations.sql
│   │   ├── ingest_cdc_alerts.sql
│   │   └── ingest_cdc_alert_thresholds.sql
│   ├── silver/                 # Materialized Views (Staging)
│   │   ├── stg_iot_events.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_devices.sql
│   │   ├── stg_locations.sql
│   │   ├── stg_alerts.sql
│   │   └── stg_alert_thresholds.sql
│   └── gold/                   # Materialized Views (Marts)
│       ├── dimensions/
│       │   ├── dim_customers.sql
│       │   ├── dim_devices.sql
│       │   ├── dim_locations.sql
│       │   └── dim_date.sql
│       ├── facts/
│       │   ├── fct_iot_events.sql
│       │   ├── fct_alerts.sql
│       │   └── fct_orders.sql
│       └── analytics/
│           ├── agg_daily_device_metrics.sql
│           ├── agg_customer_summary.sql
│           └── agg_hourly_device_metrics.sql
├── notebooks/                  # Support notebooks (jobs)
│   └── jobs/
│       └── run_data_quality.py
└── dbt/                        # Legacy dbt project (deprecated)
    └── ...
```

## Key Features

### Pure SQL Architecture
- **No Python required** for transformations
- All business logic in SQL Declarative Pipelines
- Unified lineage from ingestion to analytics

### Streaming Tables (Bronze)
- Auto Loader for file ingestion (`read_files()`)
- Schema evolution support
- Data quality expectations (CONSTRAINT ... EXPECT)
- Partitioned by ingestion date

### Materialized Views (Silver/Gold)
- CDC processing with SCD Type 1
- Automatic incremental refresh
- Built-in optimization (VACUUM, OPTIMIZE)
- Declarative data quality checks

## Prerequisites

1. **Databricks Workspace**: AWS Databricks workspace with Unity Catalog enabled
2. **Databricks CLI**: Version 0.200+ with bundle support
3. **AWS Access**: S3 access configured via instance profile or IAM role
4. **GitHub Secrets** (for CI/CD):
   - `DATABRICKS_HOST`: Your workspace URL
   - `DATABRICKS_TOKEN`: Personal access token

## Quick Start

### 1. Configure Databricks CLI

```bash
# Set up authentication
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."

# Or use ~/.databrickscfg
cat > ~/.databrickscfg << EOF
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = dapi...
EOF
```

### 2. Validate the Bundle

```bash
databricks bundle validate
```

### 3. Deploy to Development

```bash
# Deploy all resources
databricks bundle deploy -t dev

# Check deployment status
databricks bundle summary -t dev
```

### 4. Run the Pipeline

```bash
# Run the lakehouse pipeline (Bronze -> Silver -> Gold)
databricks bundle run iot_lakehouse_pipeline -t dev
```

## Pipeline Layers

### Bronze Layer (Streaming Tables)

| Table | Source | Description |
|-------|--------|-------------|
| `iot_events` | JSON files | IoT sensor events with Auto Loader |
| `cdc_customers` | CDC JSON | Customer CDC events (Debezium format) |
| `cdc_products` | CDC JSON | Product CDC events |
| `cdc_orders` | CDC JSON | Order CDC events |
| `cdc_devices` | CDC JSON | Device CDC events |
| `cdc_locations` | CDC JSON | Location CDC events |
| `cdc_alerts` | CDC JSON | Alert CDC events |
| `cdc_alert_thresholds` | CDC JSON | Threshold CDC events |

### Silver Layer (Materialized Views - Staging)

| Model | Description |
|-------|-------------|
| `stg_iot_events` | Cleaned IoT events with quality flags |
| `stg_customers` | Customer current state (SCD Type 1) |
| `stg_products` | Product catalog |
| `stg_orders` | Order transactions |
| `stg_devices` | Device inventory |
| `stg_locations` | Location master |
| `stg_alerts` | Alert events |
| `stg_alert_thresholds` | Threshold configurations |

### Gold Layer (Materialized Views - Marts)

**Dimensions:**
| Model | Description |
|-------|-------------|
| `dim_customers` | Customer dimension with metrics |
| `dim_devices` | Device dimension with health scores |
| `dim_locations` | Location dimension |
| `dim_date` | Standard date dimension |

**Facts:**
| Model | Description |
|-------|-------------|
| `fct_iot_events` | Sensor reading facts with dimension keys |
| `fct_orders` | Order transaction facts |
| `fct_alerts` | Alert facts with severity weights |

**Analytics:**
| Model | Description |
|-------|-------------|
| `agg_daily_device_metrics` | Daily device aggregations |
| `agg_hourly_device_metrics` | Hourly device metrics |
| `agg_customer_summary` | Customer health scorecard |
## Cost Optimization

This pipeline is designed for minimal cost:

1. **Single Pipeline**: All layers in one Lakeflow pipeline
2. **Spot Instances**: 100% spot with on-demand fallback
3. **Triggered Mode**: Not continuous, runs on schedule
4. **Single Node**: Development uses single node cluster
5. **No Photon**: Disabled for cost savings (enable in prod)

**Estimated Monthly Cost**: $15-25 depending on usage

## Why Lakeflow Declarative Pipelines?

| Feature | Lakeflow (New) | dbt + DLT (Old) |
|---------|----------------|-----------------|
| **Language** | Pure SQL | SQL + Python |
| **Ingestion** | Built-in (Streaming Tables) | Separate Python notebooks |
| **Transformations** | Materialized Views | dbt models |
| **Lineage** | Unified | Split between tools |
| **Maintenance** | Automatic (predictive optimization) | Manual dbt run |
| **Data Quality** | EXPECT constraints | dbt tests |
| **Complexity** | Single framework | Multiple tools |

## Data Quality

Data quality is enforced at multiple levels:

### Bronze Layer (Streaming Tables)
```sql
CONSTRAINT valid_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_sensor_type EXPECT (sensor_type IN ('temperature', 'humidity', 'pressure', 'motion'))
```

### Silver Layer (Materialized Views)
- CDC deduplication with ROW_NUMBER()
- Soft delete handling (_is_deleted flag)
- Data type casting and validation

### Gold Layer (Materialized Views)
- Referential integrity via JOINs
- Business rule validation
- Calculated health scores

## CI/CD

The pipeline uses GitHub Actions for CI/CD:

| Trigger | Action |
|---------|--------|
| PR to main | Validate bundle, lint dbt |
| Push to develop | Deploy to dev |
| Push to main | Deploy to staging |
| Release tag | Deploy to production |
| Manual | Deploy to any environment |

### Setting up CI/CD

1. Add repository secrets:
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`

2. Create environments in GitHub:
   - `development`
   - `staging`
   - `production` (with required reviewers)

## Troubleshooting

### Bundle validation fails
```bash
# Check YAML syntax
databricks bundle validate -t dev

# View detailed errors
databricks bundle deploy -t dev --debug
```

### DLT pipeline fails
```bash
# Check pipeline status
databricks pipelines get <pipeline-id>

# View events
databricks pipelines list-updates <pipeline-id>
```

### dbt connection issues
```bash
# Test connection
dbt debug --profiles-dir .

# Check credentials
echo $DATABRICKS_HOST
echo $DATABRICKS_HTTP_PATH
```

## Security Notes

1. **Credentials**: Use environment variables, not hardcoded values
2. **IAM Roles**: Prefer instance profiles over access keys
3. **Secrets**: Store in Databricks secrets scope for production
4. **Network**: Configure VPC peering for S3 access if needed

## Contributing

1. Create feature branch from `develop`
2. Make changes and test locally
3. Create PR to `develop`
4. After review, merge to `develop` (deploys to dev)
5. When ready, merge `develop` to `main` (deploys to staging)
6. Create release tag for production deployment
