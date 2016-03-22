from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from ndspark import NDS
from pyspark.streaming.kafka import KafkaUtils
import csv,sys
import pytz
import time
import pandas as pd
from datetime import datetime, date, timedelta
from pyspark.sql import HiveContext, SQLContext, Row
from pyspark.sql.types import *
from operator import add
from humanize import intcomma, naturalsize
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL as SqlAlchemyURL
from ndt.spark.context import create_spark_context
import happybase
import traceback

TZ_NY = pytz.timezone('America/New_York')
now = TZ_NY.normalize(pytz.UTC.localize(datetime.utcnow()))
nows = now.strftime("%Y-%m-%d %H:%M:%S")
jobName = 'real-time-PCA-spark-streaming'

def createNewConnection(host):
    connection = happybase.Connection(host)
    connection.open()
    return connection

def put_to_maprdb(iter):
    jobName = 'pca'
    maprDbHost = "10.21.49.3"
    tableName = "/mapr/my.cluster.com/user/bsun/tables/pca"
    connection = createNewConnection(maprDbHost)
    table = connection.table(tableName)

    for record in iter:
        try:
            update = record[0]
            row_key = record[1]
            updates = update.split(',')
            for u in updates:
                table.counter_inc(row_key,u,value=1)
        except Exception as e:
            print e
            traceback.print_exc()
    connection.close()

def mapper(row):
    try:
        key = str(row[0])
        value = str(row[1])
        values = value.split("_")
        cookies = str(values[6])
        did = str(values[7])
        return_value = values[0] + '_' + values[1] + '_' + values[2] + '_' + values[3] + '_' + values[4] + '_' + values[5]
        if cookies == '0' and did == '0':
            update = 'pca:pixel_fires'
            return (update, return_value)
        elif cookies == '1' and did == '0':
            update = 'pca:pixel_fires,pca:cookies'
            return (update, return_value)
        elif cookies == '0' and did == '1':
            update = 'pca:pixel_fires,pca:valid_deviceIds'
            return (update, return_value)
        elif cookies == '1' and did == '1':
            update = 'pca:pixel_fires,pca:valid_deviceIds,pca:cookies'
            return (update, return_value)
    except Exception as e:
        print e
        traceback.print_exc()
        return None
 
nds = NDS(jobName, cores=50, executor_memory='4g')
ssc = StreamingContext(nds.sc, 2)

brokers = "10.21.49.9:32000"
directKafkaStream = KafkaUtils.createDirectStream(ssc, ["pca"], {"metadata.broker.list": brokers})

rdd = directKafkaStream.map(mapper).filter(lambda row: row)
rdd.pprint()

rdd.foreachRDD (lambda x : x.foreachPartition(put_to_maprdb))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

