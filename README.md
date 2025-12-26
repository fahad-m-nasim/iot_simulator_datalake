# Databricks IoT Streaming Pipeline

This directory contains the Databricks Asset Bundle (DAB) for deploying and managing
the IoT streaming pipeline. It includes Delta Live Tables (DLT) for ingestion and
dbt for transformations following the medallion architecture.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              IoT Streaming Pipeline                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────┐    ┌───────────────┐    ┌───────────────┐    ┌──────────────┐ │
│  │     S3      │───▶│  Auto Loader  │───▶│    Bronze     │───▶│    Silver    │ │
│  │  (Raw Data) │    │    (DLT)      │    │  (DLT Tables) │    │ (dbt Models) │ │
│  └─────────────┘    └───────────────┘    └───────────────┘    └──────┬───────┘ │
│                                                                       │         │
│        IoT Events                       Streaming Tables              │         │
│        CDC Data                         Quality Checks                ▼         │
│                                                                 ┌──────────────┐│
│                                                                 │     Gold     ││
│                                                                 │ (dbt Marts)  ││
│                                                                 └──────────────┘│
│                                                                                  │
│  Components:                                                                     │
│  • DLT Pipeline: Bronze ingestion with Auto Loader                              │
│  • dbt Project: Silver/Gold transformations                                     │
│  • Jobs: Scheduled orchestration                                                │
│  • Cluster: Shared low-cost compute                                             │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
databricks/
├── databricks.yml              # Main DAB configuration
├── resources/
│   ├── clusters.yml            # Cluster definitions (shared compute)
│   ├── pipelines.yml           # DLT pipeline for Bronze ingestion
│   └── jobs.yml                # Job definitions
├── notebooks/
│   ├── bronze/
│   │   ├── ingest_iot_events.py    # DLT: IoT event ingestion
│   │   └── ingest_cdc_tables.py    # DLT: CDC table ingestion
│   └── jobs/
│       ├── run_dbt_deps.py         # Install dbt packages
│       ├── run_dbt_build.py        # Run dbt models
│       ├── run_dbt_test.py         # Run dbt tests
│       └── run_data_quality.py     # Data quality checks
└── dbt/
    ├── dbt_project.yml         # dbt project config
    ├── profiles.yml            # Databricks connection
    ├── packages.yml            # dbt packages
    ├── models/
    │   ├── staging/            # Silver layer (CDC processing)
    │   └── marts/              # Gold layer
    │       ├── dimensions/     # Dimension tables
    │       ├── facts/          # Fact tables
    │       └── analytics/      # Aggregation tables
    └── tests/
        ├── generic/            # Reusable test macros
        ├── data_quality/       # Data quality tests
        └── integration/        # Integration tests
```

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
cd databricks
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
# Trigger DLT ingestion
databricks bundle run iot_ingestion_job -t dev

# Run dbt transformations
databricks bundle run dbt_transformation_job -t dev

# Run data quality checks
databricks bundle run data_quality_job -t dev
```

## Environment Configuration

| Target | Catalog | Schema Prefix | Compute | Schedule |
|--------|---------|---------------|---------|----------|
| dev | iot_streaming_dev | dev | i3.xlarge (spot) | Hourly |
| staging | iot_streaming_staging | staging | i3.xlarge (spot) | 30 min |
| prod | iot_streaming_prod | prod | i3.xlarge (spot) | 15 min |

## Cost Optimization

This pipeline is designed for minimal cost:

1. **Shared Cluster**: Single cluster used across all jobs
2. **Spot Instances**: 100% spot with on-demand fallback
3. **Auto-termination**: 10-20 minutes based on environment
4. **Triggered DLT**: Not continuous, runs on schedule
5. **No Serverless**: All compute is classic clusters
6. **Small Instance Type**: i3.xlarge for cost-effective Spark

**Estimated Monthly Cost**: $15-25 depending on usage

## dbt Models

### Staging (Silver Layer)
- `stg_iot_events`: Cleaned IoT sensor events
- `stg_customers`: Customer current state (SCD Type 1)
- `stg_products`: Product catalog
- `stg_orders`: Order transactions
- `stg_devices`: Device inventory
- `stg_locations`: Location master
- `stg_alerts`: Alert events
- `stg_alert_thresholds`: Threshold configurations

### Marts (Gold Layer)

**Dimensions:**
- `dim_customers`: Customer dimension with metrics
- `dim_devices`: Device dimension with health scores
- `dim_locations`: Location dimension
- `dim_date`: Standard date dimension

**Facts:**
- `fct_iot_events`: Sensor reading facts
- `fct_orders`: Order transaction facts
- `fct_alerts`: Alert facts

**Analytics:**
- `agg_daily_device_metrics`: Daily device aggregations
- `agg_customer_summary`: Customer health scorecard

## Running dbt Locally

```bash
cd databricks/dbt

# Set environment variables
export DATABRICKS_HOST="your-workspace"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/..."
export DBT_CATALOG="iot_streaming_dev"
export DBT_ENV_PREFIX="dev"

# Install dependencies
dbt deps

# Run models
dbt run

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

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
