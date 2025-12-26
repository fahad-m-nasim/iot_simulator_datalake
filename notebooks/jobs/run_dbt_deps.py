# Databricks notebook source
# MAGIC %md
# MAGIC # dbt deps - Install Dependencies
# MAGIC 
# MAGIC This notebook installs dbt packages defined in packages.yml

# COMMAND ----------

# MAGIC %pip install dbt-databricks==1.8.0

# COMMAND ----------

import os
import subprocess

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "iot_streaming_dev")
dbutils.widgets.text("schema_prefix", "dev")

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")

# COMMAND ----------

# Set environment variables
os.environ["DBT_CATALOG"] = catalog
os.environ["DBT_ENV_PREFIX"] = schema_prefix
os.environ["DATABRICKS_HOST"] = spark.conf.get("spark.databricks.workspaceUrl")
os.environ["DATABRICKS_TOKEN"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

# Change to dbt project directory
# The bundle deploys files to: /Workspace/Users/{user}/.bundle/{bundle_name}/{target}/files/
dbt_project_path = None

try:
    # Get current notebook path to determine bundle location
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    print(f"Notebook path: {notebook_path}")
    
    # The notebook is at: /Users/{user}/.bundle/{bundle}/{target}/files/notebooks/jobs/run_dbt_deps
    # The dbt folder is at: /Users/{user}/.bundle/{bundle}/{target}/files/dbt
    if '/.bundle/' in notebook_path:
        # Extract path up to 'files' and append 'dbt'
        parts = notebook_path.split('/files/')
        if len(parts) > 1:
            dbt_project_path = f"/Workspace{parts[0]}/files/dbt"
except Exception as e:
    print(f"Warning: Could not determine path from context: {e}")

# If path detection failed or path doesn't exist, try alternatives
alternatives = [
    dbt_project_path,
    "/Workspace/Repos/iot-streaming-pipeline/databricks/dbt",
    "/dbfs/FileStore/dbt/iot_streaming_pipeline"
]

for path in alternatives:
    if path and os.path.exists(path):
        dbt_project_path = path
        break

if not dbt_project_path or not os.path.exists(dbt_project_path):
    print(f"ERROR: Could not find dbt project! Tried: {alternatives}")
    print(f"Listing /Workspace contents to debug...")
    for root, dirs, files in os.walk("/Workspace", topdown=True):
        if 'dbt' in dirs:
            print(f"Found dbt at: {root}/dbt")
        if len(root.split('/')) > 8:  # Limit depth
            del dirs[:]
    raise Exception(f"dbt project path not found!")

print(f"dbt project path: {dbt_project_path}")
print(f"Contents: {os.listdir(dbt_project_path)}")

# COMMAND ----------

# Run dbt deps
os.chdir(dbt_project_path)
print(f"Working directory: {os.getcwd()}")
print(f"Contents: {os.listdir('.')}")
print(f"dbt_project.yml exists: {os.path.exists('dbt_project.yml')}")
print(f"packages.yml exists: {os.path.exists('packages.yml')}")

# Show packages.yml content
if os.path.exists('packages.yml'):
    with open('packages.yml', 'r') as f:
        print("=== packages.yml content ===")
        print(f.read())

# Check dbt version
version_result = subprocess.run(
    ["dbt", "--version"],
    capture_output=True,
    text=True
)
print("=== dbt version ===")
print(version_result.stdout)
print(version_result.stderr)

# Create a writable temp directory for dbt packages
import tempfile
import shutil

# Copy dbt project to temp directory where we have write access
temp_dbt_dir = "/tmp/dbt_project"
if os.path.exists(temp_dbt_dir):
    shutil.rmtree(temp_dbt_dir)
shutil.copytree(dbt_project_path, temp_dbt_dir)

os.chdir(temp_dbt_dir)
print(f"Using temp directory: {temp_dbt_dir}")
print(f"Contents: {os.listdir('.')}")

# Now run dbt deps in temp directory (this doesn't need database connection)
result = subprocess.run(
    ["dbt", "deps", "--profiles-dir", "."],
    capture_output=True,
    text=True,
    env={**os.environ, "DBT_PROFILES_DIR": temp_dbt_dir}
)

print("=== dbt deps output ===")
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
print(f"Return code: {result.returncode}")

if result.returncode != 0:
    # Print more diagnostic info before failing
    print("=== Additional diagnostics ===")
    print(f"PATH: {os.environ.get('PATH', 'not set')}")
    print(f"HOME: {os.environ.get('HOME', 'not set')}")
    print(f"USER: {os.environ.get('USER', 'not set')}")
    
    # Check if dbt_packages folder exists
    print(f"dbt_packages exists: {os.path.exists('dbt_packages')}")
    if os.path.exists('dbt_packages'):
        print(f"dbt_packages contents: {os.listdir('dbt_packages')}")
    
    raise Exception(f"dbt deps failed with return code {result.returncode}")

# Copy dbt_packages back to workspace path (for subsequent runs)
packages_dir = os.path.join(temp_dbt_dir, "dbt_packages")
if os.path.exists(packages_dir):
    print(f"dbt_packages installed: {os.listdir(packages_dir)}")
    # Store path for other tasks
    dbutils.jobs.taskValues.set(key="dbt_project_path", value=temp_dbt_dir)

# COMMAND ----------

print("dbt deps completed successfully!")
