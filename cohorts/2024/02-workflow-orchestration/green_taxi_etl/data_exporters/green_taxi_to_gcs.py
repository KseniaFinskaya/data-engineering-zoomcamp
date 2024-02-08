import os
import pyarrow as pa
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/home/src/vivid-layout-412703-2dd87aa741af.json"

bucket_name="ksenia-finskaya-green-taxi"
# object_key="green_taxi.parquet"
project_id="vivid-layout-412703"
table_name="green_taxi"

root_path=f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, **kwargs) -> None:
    table = pa.Table.from_pandas(data)
    gcs=pa.fs.GcsFileSystem()
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
 