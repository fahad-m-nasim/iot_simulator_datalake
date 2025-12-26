brew tap dbt-labs/dbt-cli
brew install dbt


conda create -n iot_simulator python==3.14
conda activate iot_simulator
pip install pyspark==4.0.0
pip install dbt-core dbt-databricks
dbt init iot_simulator_datalake