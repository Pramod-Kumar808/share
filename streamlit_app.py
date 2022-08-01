import mysql.connector
import streamlit as st
import pandas as pd
import numpy as np
import time
import plotly.express as px
import plotly

st.set_page_config(
    page_title = 'Company Name',
    page_icon = '✅',
    layout = 'wide'
)

st.title("Real time data streaming")

# Connecting to Monogdb server and database
def mysql_connect() -> (None):
    try:
        connection = mysql.connector.connect(host="localhost", user="root", password="root", database = "random_database")
        return connection

    except Exception as error:
        print("Error connecting Mongo: ", error)

connet_sql = mysql_connect()
cursor = connet_sql.cursor()

query = "select * from generated_data"
cursor.execute(query)
query_result = cursor.fetchall()

date = []
value1 = []
value2 =  []

for x in query_result:
    date.append(x[0])
    value1.append(float(x[1]))
    value2.append(float(x[2]))

pd_df = pd.DataFrame({"timedate" : date, "value1" : value1, "value2" : value2})
last_10_records = pd_df.tail(10)
last_records = pd_df.tail(1)

placeholder = st.empty()


# while True:  This is for the infinity loop time
for time_sec in range(20):

    pd_df["value1"] = pd_df["value1"]
    pd_df["value2"] = pd_df["value2"]

    avg_value1 = np.mean(pd_df["value1"])
    min_value1 = np.min(pd_df["value1"])
    max_value1 = np.max(pd_df["value1"])

    avg_value2 = np.mean(pd_df["value2"])
    min_value2 = np.min(pd_df["value2"])
    max_value2 = np.max(pd_df["value2"])

    with placeholder.container():
        kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)

        kpi2.metric(label = "Average Value1 ⏳", value = round(avg_value1,2), delta = round(avg_value1,2))
        kpi3.metric(label = "Maximum Value1 ⏳", value = round(max_value1,2))
        kpi1.metric(label = "Minimum Value1 ⏳", value = round(min_value1,2))

        kpi5.metric(label = "Average Value2 ⏳", value = round(avg_value2,2), delta = round(avg_value2,2))
        kpi6.metric(label = "Maximum Value1 ⏳", value = round(max_value2,2))
        kpi4.metric(label = "Minimum Value1 ⏳", value = round(min_value2,2))

        st.title(" Streaming Chart")
        fig_1, fig_2 = st.columns(2)
        with fig_1:
            st.markdown(" Chart of value1")
            fig = px.line(data_frame = pd_df, y = "value1")
            st.write(fig, use_column_width=True)

        with fig_2:
            st.markdown(" Chart of value2")
            fig = px.line(data_frame = pd_df, y = "value2")
            st.write(fig, use_column_width=True)

        st.title(" Detailed Data View")
        table1, table2, table3 = st.columns(3)
        with table1:
            st.markdown(" Table 1 Last records")
            st.dataframe(last_records)
        
        with table2:
            st.markdown(" Table 2 Last 10 records")
            st.dataframe(last_10_records)

        with table3:
            st.markdown(" Table 3 All records")
            st.dataframe(pd_df)

        time.sleep(1)

