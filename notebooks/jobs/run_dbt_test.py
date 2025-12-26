# Databricks notebook source
# MAGIC %md
# MAGIC # dbt test - Run Tests
# MAGIC 
# MAGIC This notebook runs all dbt tests (schema + data + custom)

# COMMAND ----------

# MAGIC %pip install dbt-databricks==1.8.0

# COMMAND ----------

import os
import subprocess

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "iot_streaming_dev")
dbutils.widgets.text("schema_prefix", "dev")
dbutils.widgets.text("select", "")  # Optional: specific tests to run

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
select = dbutils.widgets.get("select")

# COMMAND ----------

# Set environment variables
os.environ["DBT_CATALOG"] = catalog
os.environ["DBT_ENV_PREFIX"] = schema_prefix
os.environ["DATABRICKS_HOST"] = spark.conf.get("spark.databricks.workspaceUrl")
os.environ["DATABRICKS_TOKEN"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

http_path = spark.conf.get("spark.databricks.clusterUsageTags.clusterHttpPath", "/sql/1.0/warehouses/starter")
os.environ["DATABRICKS_HTTP_PATH"] = http_path

# COMMAND ----------

# Change to dbt project directory - use temp directory
import shutil

temp_dbt_dir = "/tmp/dbt_project"
dbt_project_path = None

# First check if deps/build task already set up the temp directory
if os.path.exists(temp_dbt_dir) and os.path.exists(os.path.join(temp_dbt_dir, "dbt_packages")):
    dbt_project_path = temp_dbt_dir
    print(f"Using existing temp directory: {dbt_project_path}")
else:
    # Find the original dbt project path from bundle
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        print(f"Notebook path: {notebook_path}")
        
        if '/.bundle/' in notebook_path:
            parts = notebook_path.split('/files/')
            if len(parts) > 1:
                bundle_dbt_path = f"/Workspace{parts[0]}/files/dbt"
                if os.path.exists(bundle_dbt_path):
                    if os.path.exists(temp_dbt_dir):
                        shutil.rmtree(temp_dbt_dir)
                    shutil.copytree(bundle_dbt_path, temp_dbt_dir)
                    dbt_project_path = temp_dbt_dir
    except Exception as e:
        print(f"Warning: {e}")

    if not dbt_project_path:
        alternatives = ["/Workspace/Repos/iot-streaming-pipeline/databricks/dbt"]
        for path in alternatives:
            if os.path.exists(path):
                if os.path.exists(temp_dbt_dir):
                    shutil.rmtree(temp_dbt_dir)
                shutil.copytree(path, temp_dbt_dir)
                dbt_project_path = temp_dbt_dir
                break

if not dbt_project_path or not os.path.exists(dbt_project_path):
    raise Exception(f"dbt project path not found!")

os.chdir(dbt_project_path)
print(f"Working directory: {os.getcwd()}")

# Ensure packages are installed
if not os.path.exists("dbt_packages"):
    print("Installing dbt packages...")
    deps_result = subprocess.run(["dbt", "deps", "--profiles-dir", "."], capture_output=True, text=True)
    print(deps_result.stdout)
    if deps_result.returncode != 0:
        print(deps_result.stderr)
        raise Exception(f"dbt deps failed: {deps_result.returncode}")

# COMMAND ----------

# Build test command
cmd = ["dbt", "test", "--profiles-dir", "."]

if select:
    cmd.extend(["--select", select])

print(f"Running: {' '.join(cmd)}")

# COMMAND ----------

# Run dbt test
result = subprocess.run(cmd, capture_output=True, text=True)

print("=" * 50)
print("TEST RESULTS:")
print("=" * 50)
print(result.stdout)

if result.stderr:
    print("STDERR:")
    print(result.stderr)

# COMMAND ----------

# Parse test results for reporting
import re

# Count passes and failures
pass_count = len(re.findall(r'PASS', result.stdout))
warn_count = len(re.findall(r'WARN', result.stdout))
fail_count = len(re.findall(r'FAIL', result.stdout))
error_count = len(re.findall(r'ERROR', result.stdout))

print(f"\n{'=' * 50}")
print("TEST SUMMARY")
print(f"{'=' * 50}")
print(f"Passed: {pass_count}")
print(f"Warnings: {warn_count}")
print(f"Failed: {fail_count}")
print(f"Errors: {error_count}")

# COMMAND ----------

if result.returncode != 0:
    print(f"\n⚠️ Some tests failed (return code: {result.returncode})")
    # Don't fail the job for warnings, only for failures/errors
    if fail_count > 0 or error_count > 0:
        raise Exception(f"dbt test failed: {fail_count} failures, {error_count} errors")
else:
    print("\n✅ All tests passed!")
