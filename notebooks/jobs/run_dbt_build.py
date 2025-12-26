# Databricks notebook source
# MAGIC %md
# MAGIC # dbt build - Run Models
# MAGIC 
# MAGIC This notebook runs dbt models (compile + run + test)

# COMMAND ----------

# MAGIC %pip install dbt-databricks==1.8.0

# COMMAND ----------

import os
import subprocess

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "iot_streaming_dev")
dbutils.widgets.text("schema_prefix", "dev")
dbutils.widgets.text("full_refresh", "false")
dbutils.widgets.text("select", "")  # Optional: specific models to run

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"
select = dbutils.widgets.get("select")

# COMMAND ----------

# Set environment variables
os.environ["DBT_CATALOG"] = catalog
os.environ["DBT_ENV_PREFIX"] = schema_prefix
os.environ["DATABRICKS_HOST"] = spark.conf.get("spark.databricks.workspaceUrl")
os.environ["DATABRICKS_TOKEN"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Get SQL warehouse HTTP path for dbt connection
# Use the cluster's HTTP path
http_path = spark.conf.get("spark.databricks.clusterUsageTags.clusterHttpPath", "/sql/1.0/warehouses/starter")
os.environ["DATABRICKS_HTTP_PATH"] = http_path

print(f"Catalog: {catalog}")
print(f"Schema prefix: {schema_prefix}")
print(f"Full refresh: {full_refresh}")
print(f"HTTP path: {http_path}")

# COMMAND ----------

# Change to dbt project directory
# Try to get temp path from deps task, otherwise find from bundle location
import shutil

temp_dbt_dir = "/tmp/dbt_project"
dbt_project_path = None

# First check if deps task already set up the temp directory
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
                    # Copy to temp directory
                    if os.path.exists(temp_dbt_dir):
                        shutil.rmtree(temp_dbt_dir)
                    shutil.copytree(bundle_dbt_path, temp_dbt_dir)
                    dbt_project_path = temp_dbt_dir
                    print(f"Copied bundle dbt project to: {dbt_project_path}")
    except Exception as e:
        print(f"Warning: Could not determine path from context: {e}")
    
    # If still not found, try alternatives
    if not dbt_project_path:
        alternatives = [
            "/Workspace/Repos/iot-streaming-pipeline/databricks/dbt",
            "/dbfs/FileStore/dbt/iot_streaming_pipeline"
        ]
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
print(f"Contents: {os.listdir('.')}")

# Run dbt deps if packages not installed
if not os.path.exists("dbt_packages"):
    print("Installing dbt packages...")
    deps_result = subprocess.run(["dbt", "deps", "--profiles-dir", "."], capture_output=True, text=True)
    print(deps_result.stdout)
    if deps_result.returncode != 0:
        print(deps_result.stderr)
        raise Exception(f"dbt deps failed: {deps_result.returncode}")

# COMMAND ----------

# Build dbt command
cmd = ["dbt", "build", "--profiles-dir", "."]

if full_refresh:
    cmd.append("--full-refresh")

if select:
    cmd.extend(["--select", select])

print(f"Running: {' '.join(cmd)}")

# COMMAND ----------

# Run dbt build
result = subprocess.run(cmd, capture_output=True, text=True)

print("=" * 50)
print("STDOUT:")
print("=" * 50)
print(result.stdout)

print("=" * 50)
print("STDERR:")
print("=" * 50)
print(result.stderr)

# COMMAND ----------

if result.returncode != 0:
    raise Exception(f"dbt build failed with return code {result.returncode}")

print("dbt build completed successfully!")
