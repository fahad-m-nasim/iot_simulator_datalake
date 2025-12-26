# dbt Testing Strategy

This document outlines the comprehensive testing strategy for the IoT Streaming Pipeline dbt project, following enterprise best practices.

## Testing Pyramid

```
                    ┌─────────────────┐
                    │   E2E Tests     │  ← Data Quality Jobs (Daily)
                    │   (Slowest)     │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Integration    │  ← Cross-model tests (On deploy)
                    │     Tests       │
                    └────────┬────────┘
                             │
           ┌─────────────────▼─────────────────┐
           │         Schema Tests              │  ← Generic tests (On dbt build)
           │   (not_null, unique, accepted)    │
           └─────────────────┬─────────────────┘
                             │
    ┌────────────────────────▼────────────────────────┐
    │              Unit Tests (Fastest)               │  ← On PR (dbt 1.8+)
    │     Validate SQL logic without database         │
    └─────────────────────────────────────────────────┘
```

## Test Types

### 1. Unit Tests (dbt 1.8+)
**Location:** `tests/unit/unit_tests.yml`
**When:** On every Pull Request
**Purpose:** Validate SQL transformation logic in isolation

```yaml
unit_tests:
  - name: test_battery_status_classification
    model: stg_iot_events
    given:
      - input: ref('iot_events_validated')
        rows:
          - {battery_level: 15}  # Critical
          - {battery_level: 35}  # Low
    expect:
      rows:
        - {battery_status: "critical"}
        - {battery_status: "low"}
```

**Benefits:**
- Fast execution (no database needed)
- Test edge cases easily
- TDD approach for complex transformations
- Run in CI without Databricks connection

### 2. Schema Tests (Generic Tests)
**Location:** `models/**/_schema.yml`
**When:** On every `dbt build` / `dbt test`
**Purpose:** Validate data integrity

```yaml
models:
  - name: stg_iot_events
    columns:
      - name: event_id
        tests:
          - not_null
          - unique
      - name: sensor_type
        tests:
          - accepted_values:
              values: ['temperature', 'humidity', 'pressure', 'motion']
      - name: battery_level
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
```

### 3. Custom Generic Tests
**Location:** `tests/generic/`
**When:** On every `dbt test`
**Purpose:** Reusable business logic tests

```sql
-- tests/generic/positive_value.sql
{% test positive_value(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} <= 0
  AND {{ column_name }} IS NOT NULL
{% endtest %}
```

**Usage:**
```yaml
- name: sensor_value
  tests:
    - positive_value
```

### 4. Data Quality Tests
**Location:** `tests/data_quality/`
**When:** Scheduled daily
**Purpose:** Validate data freshness and consistency

```sql
-- tests/data_quality/test_iot_events_freshness.sql
{% set freshness_threshold_hours = 6 %}

SELECT *
FROM {{ ref('fct_iot_events') }}
WHERE TIMESTAMPDIFF(HOUR, event_timestamp, CURRENT_TIMESTAMP()) > {{ freshness_threshold_hours }}
```

### 5. Integration Tests
**Location:** `tests/integration/`
**When:** On staging/prod deployment
**Purpose:** Cross-model relationship validation

```sql
-- tests/integration/test_device_event_consistency.sql
WITH fact_counts AS (
    SELECT device_key, COUNT(*) AS fact_count
    FROM {{ ref('fct_iot_events') }}
    GROUP BY device_key
)
SELECT *
FROM {{ ref('dim_devices') }} d
LEFT JOIN fact_counts f ON d.device_key = f.device_key
WHERE d.total_events != COALESCE(f.fact_count, 0)
```

## Test Configuration

### Severity Levels
```yaml
# dbt_project.yml
tests:
  +severity: warn           # Default: warn (doesn't fail build)
  +store_failures: true     # Store failed rows for debugging
  +schema: "test_results_{{ var('env_prefix') }}"
```

Override for critical tests:
```yaml
- name: event_id
  tests:
    - not_null:
        severity: error     # Fails the build if violated
```

### Source Freshness
```yaml
sources:
  - name: bronze
    freshness:
      warn_after: {count: 2, period: hour}
      error_after: {count: 6, period: hour}
    loaded_at_field: _ingested_at
```

Run with: `dbt source freshness`

## Running Tests

### Local Development
```bash
# Run all tests
dbt test

# Run only unit tests (fast, no DB needed)
dbt test --select "test_type:unit"

# Run tests for specific model
dbt test --select stg_iot_events

# Run tests for changed models only (slim CI)
dbt test --select "state:modified+" --defer --state ./prod-manifest/
```

### CI/CD Integration
```yaml
# .github/workflows/dbt-ci.yml
- name: Run Unit Tests
  run: dbt test --select "test_type:unit"

- name: Run Schema Tests
  run: dbt test --exclude "test_type:unit"
```

### Databricks Jobs
```yaml
# jobs.yml - dbt_transformation_job
tasks:
  - task_key: "dbt_build"
    # Runs compile + run + test
    run: dbt build --fail-fast

  - task_key: "dbt_test"
    depends_on: [dbt_build]
    # Additional integration tests
    run: dbt test --select "tag:integration"
```

## Best Practices

### 1. Test Coverage Goals
| Layer | Minimum Coverage | Critical Tests |
|-------|------------------|----------------|
| Staging | 80% columns | not_null, unique on PKs |
| Intermediate | 60% columns | Business logic validation |
| Marts | 100% columns | All PKs, FKs, aggregations |

### 2. Naming Conventions
- Unit tests: `test_<model>_<scenario>`
- Integration tests: `test_<fact>_<dimension>_consistency`
- Data quality: `test_<metric>_freshness` / `test_no_duplicate_<entity>`

### 3. Test Documentation
```yaml
- name: test_device_event_consistency
  description: |
    Ensures dim_devices.total_events matches the actual count
    in fct_iot_events. Failures indicate a sync issue.
  config:
    severity: error
    tags: ['integration', 'critical']
```

### 4. Failure Investigation
```sql
-- Query failed test rows
SELECT * FROM test_results_dev.test_iot_events_freshness
ORDER BY _dbt_test_run_at DESC
LIMIT 100;
```

## Test Metrics Dashboard

Track test health in Databricks SQL:
```sql
-- Test pass rate over time
SELECT 
    DATE(run_started_at) AS run_date,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS passed,
    ROUND(100.0 * SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pass_rate
FROM dbt_run_results
WHERE resource_type = 'test'
GROUP BY 1
ORDER BY 1 DESC;
```

## Environment-Specific Testing

| Environment | Tests Run | Frequency |
|-------------|-----------|-----------|
| **Dev** | Unit + Schema | On commit |
| **Staging** | All + Integration | On merge to main |
| **Prod** | Data Quality | Hourly schedule |

Configure in dbt_project.yml:
```yaml
tests:
  iot_streaming_pipeline:
    +enabled: "{{ target.name != 'prod' or var('run_tests_in_prod', false) }}"
    data_quality:
      +enabled: true  # Always run data quality
```
