import json
import os

from kafka import KafkaConsumer
from json import loads
from s3fs import S3FileSystem
from google.cloud import bigquery

consumer = KafkaConsumer(
    'Round1Test',
    bootstrap_servers = os.environ.get('AWS_EC2_Server'),
    value_deserializer = lambda x:
        loads(x.decode('utf-8'))
    )

"""
s3 = S3FileSystem()
s3Bucket = "s3://{bucketPath}"
for count, i in enumerate(consumer):
    with s3.open(s3Bucket + f"live_tournament_{count}.json", 'w') as file:
        json.dump(i.value, file)
"""

client = bigquery.Client.from_service_account_json(
    "./personal-pgatour-gbq-service-account.json"
    )
job_config = bigquery.LoadJobConfig(
    schema = [
        bigquery.SchemaField("timestampScore"  , bigquery.enums.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("PlayerRoundID"   , bigquery.enums.SqlTypeNames.INTEGER  ),
        bigquery.SchemaField("HoleNumber"      , bigquery.enums.SqlTypeNames.INTEGER  ),
        bigquery.SchemaField("Par"             , bigquery.enums.SqlTypeNames.INTEGER  ),
        bigquery.SchemaField("ScrambledStrokes", bigquery.enums.SqlTypeNames.INTEGER  ),
        bigquery.SchemaField("ToPar"           , bigquery.enums.SqlTypeNames.INTEGER  ),
        bigquery.SchemaField("Strokes"         , bigquery.enums.SqlTypeNames.INTEGER  ),
        ]
    )

for count, i in enumerate(consumer):
    job = client.load_table_from_json(
        json.dump(i),
         "{gbq_landing_zone}",
        job_config = job_config
        )