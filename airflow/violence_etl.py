"""_summary_

    Returns:
        _type_: _description_
"""
# Import necessary libraries
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from utils.firestore_database import FirestoreData

URI = "mongodb+srv://piwindy21:0ydJGb6vT6BZBCbH@cluster0.bfxzxxb.mongodb.net/"
DB_NAME = "cluster0.bfxzxxb.mongodb.net"
COLLECTION = "violence_report"

class MongoDB:
    """_summary_
    """
    def __init__(self, uri, dbname, collection_name):
        self.client = MongoClient(uri, server_api=ServerApi('1'))
        self._db = self.client[dbname]
        self.collection = self._db[collection_name]

    def get_all_documents(self):
        return list(self.collection.find())

    def delete_all_documents(self):
        self.collection.delete_many({})

    def get_item_by_field(self, field, value):
        return list(self.collection.find({field: value}))

    def add_document(self, document):
        self.collection.insert_one(document)

    def count_documents(self):
        return self.collection.count_documents({})


# Firestore
DATA_INDEX = 'violence-data'
#RP_INDEX = 'violence-report'
es_tract = FirestoreData()
#mongoDB
mongodb = MongoDB(uri=URI, dbname=DB_NAME, collection_name=COLLECTION)

# Create SparkSession
spark = SparkSession.builder \
    .appName("PySpark ETL with GCP") \
    .getOrCreate()


def extract():
    # Define schema for the incoming dataframe
    schema = StructType([
        StructField("predicted_class_name", StringType()),
        StructField("predicted_labels_probabilities", DoubleType()),
        StructField("saved_filename", StringType()),
        StructField("timestamp", StringType()),
        StructField('location', StructType([
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType())
        ]))
    ])
    df = []
    # Load data from Data Source
    for hit in es_tract.get_all(collection_name=DATA_INDEX):
        df.append(hit.to_dict())
    df = spark.createDataFrame(data=df, schema=schema)
    return df


def transform(df):
    data = df.withColumn("datetime", to_timestamp("timestamp", "yyyy/MM/dd HH:mm:ss"))
    # df2.printSchema()
    data = data.na.drop(how='any')
    # Extract the relevant information and group by date, month, longitude, and latitude

    processed_df = data.select(
        to_date("datetime").alias("date"),
        month("datetime").alias("month"),
        col("location.lon").alias("longitude"),
        col("location.lat").alias("latitude"),
        col("predicted_class_name").alias("violent_incident"),
        col("predicted_labels_probabilities").alias("probabilities")) \
        .groupBy("date", "month", "longitude", "latitude") \
        .agg(count("violent_incident").alias("total_violence"),
             round(mean("probabilities"), 4).alias("avg_probabilities"))

    # Show the processed data
    processed_df.orderBy(col('date').asc(), col("total_violence").desc()).show(truncate=False)
    return processed_df.withColumn("date", processed_df["date"].cast(StringType()))


def load(df_transformed):
    for row in df_transformed.collect():
        mongodb.add_document(document=row.asDict(True))

if __name__ == '__main__':
    df = extract()
    df_transformed = transform(df)
    load(df_transformed=df_transformed)
