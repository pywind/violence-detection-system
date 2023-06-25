import json
from datetime import datetime
from datetime import timedelta

import streamlit as st
# import pandas as pd
# import plotly.express as px
# from time import sleep
from annotated_text import annotated_text

from utils.firestore_database import FirestoreData
from utils.kafkaUtils import KafkaHandler

st.set_page_config(
  page_title="Real-Time Violence Alert Dashboard",
  page_icon="✅",
  layout="wide",
)


# Define a function to update the time at regular intervals
def update_time():
  now = datetime.now()
  dt_string = now.strftime("%d %B %Y %H:%M:%S")
  st.write(f"Last violence update: {dt_string}")


@st.cache_resource()
def init_kafka():
  handler_consumer = KafkaHandler(is_consumer=True)
  return handler_consumer


@st.cache_resource()
def init_datastore():
  db = FirestoreData()
  return db


def query_datastore_by_timestamp(datastore: FirestoreData, timestamp) -> int:
  return len(datastore.get_item_by_key_value('violence-data', "timestamp", timestamp))


def total_violations(datastore: FirestoreData, collection_name) -> int:
  return datastore.count_all(collection_name)


def main():
  st.sidebar.title("Violence Detection Simulator")
  st.sidebar.text("Violence system\nin realtime using Spark & Kafka")
  
  st.header("Violence Recent Changes")
  
  fs = init_datastore()
  handler_consumer = init_kafka()
  total = total_violations(fs, collection_name='violence-data')
  
  today = datetime.today()
  yesterday = today - timedelta(1)
  today = today.strftime("%Y/%m/%d")
  yesterday = yesterday.strftime("%Y/%m/%d")
  
  activities = ["Realtime", "Statistics"]
  
  choice = st.sidebar.selectbox(
    "Choose among the given options:", activities)
  
  link = '[©Developed by 19133045-19133037](http://github.com/pywind)'
  st.sidebar.markdown(link, unsafe_allow_html=True)
  
  if choice == 'Realtime':
    
    with st.expander("See realtime violence updates for more information"):
      for message in handler_consumer.consume_messages():
        if message is not None:
          update_time()
          data = json.loads(message)
          annotated_text(
            "Violence",
            ("in", data["timestamp"], "#8ef"),
            " with probability ",
            ("is", str(data["predicted_labels_probabilities"]), "#faa"),
            "and",
            ("stored in ", data["saved_filename"], "#afa"),
            "where located in",
            ("lat", str(data["location"]["lat"]), "#fea"),
            " and ",
            ("lon", str(data["location"]["lon"]), "#8ef"),
          )
  if choice == "Statistics":
    number_today = query_datastore_by_timestamp(fs, today)
    number_yesterday = query_datastore_by_timestamp(fs, yesterday)
    
    m1, m2, m3 = st.columns(3)
    # fill in those three columns with respective metrics or KPIs
    m1.metric(
      label="Number of violence to day",
      value=number_today,
      delta=number_today,
    )
    
    m2.metric(
      label="Number of violence last day",
      value=number_yesterday,
      delta=number_yesterday - number_today,
    )
    
    m3.metric(
      label="Total violence all time",
      value=total
    )


if __name__ == "__main__":
  main()
