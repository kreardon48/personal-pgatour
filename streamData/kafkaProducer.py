#%%

import boto3
import os

import pandas   as pd

from kafka import KafkaProducer
from json import dumps
from time import sleep

producer = KafkaProducer(
    bootstrap_servers = os.environ.get('AWS_EC2_Server'),
    value_serializer = lambda x:
        dumps(x).encode('utf-8')
    )

dfRound1 = (
    pd.read_csv("./pgatour-phase3/streamData/players_hole_stream_round1.csv")
      .drop(columns = ['Unnamed: 0'])
    )
dfRound1_Test = dfRound1[dfRound1.index <= 1000][:]
for streamTime in list(dfRound1_Test['timestampScore'].unique()):
    dfTmp = dfRound1_Test[dfRound1_Test['timestampScore'] == streamTime][:]
    for idx in range(0, len(dfTmp.index)):
        dictRow = dfTmp.iloc[[idx]].to_dict(orient = "records")[0]
        producer.send('Round1Test', value = dictRow)
        sleep(0.1)
    
    sleep(5)
    
#%%
dfRound1 = (
    pd.read_csv("./streamData/players_hole_stream_round1.csv")
      .drop(columns = ['Unnamed: 0'])
    )
dfRound1_Test = dfRound1[dfRound1.index <= 1000][:]
for streamTime in list(dfRound1_Test['timestampScore'].unique()):
    dfTmp = dfRound1_Test[dfRound1_Test['timestampScore'] == streamTime][:]
    for idx in range(0, len(dfTmp.index)):
        dictRow = dfTmp.iloc[[idx]].to_dict(orient = "records")[0]
        push2kinesis = [
            {
                'Data': dumps(dictRow),
                'PartitionKey' : str(hash(dictRow['PlayerRoundID']))
                }
            ]
        boto3.client('kinesis').put_records(
            Records = push2kinesis,
            StreamName = 'Round1Testing', StreamARN = "{}:stream/Round1Testing".format(
                os.environ.get("AWS_KINESIS_ARN")
                )
            )
        sleep(0.1)
    
    sleep(5)

#%%


import boto3
import os

import pandas as pd
import datetime as dt

from google.cloud import bigquery
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
from time import sleep

os.chdir(
    "/Users/reardonfamily/Desktop/kylewreardon_dataportfolio"
    )

dfRound1 = (
    pd.read_csv("./pgatour-phase3/streamData/players_hole_stream_round1.csv")
      .drop(columns = ['Unnamed: 0'])
    )
dfTest = dfRound1.head(1)
dictRow = dfTest.to_dict(orient = "records")[0]

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

job = client.load_table_from_json(
    dictRow,
    "{gbq_landing_zone}",
    job_config = job_config
    )
job.result()