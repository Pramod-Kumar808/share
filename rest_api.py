from flask import Flask, jsonify
import pandas as pd
import received_rabbitmq

app = Flask(__name__)

#Connect to the mysql and display the data
connet_sql = received_rabbitmq.mysql_connect()
cursor = connet_sql.cursor()

@app.get("/last_point")
def get_last_point_of_data():
    query = "select * from generated_data ORDER BY datetime DESC LIMIT 1"
    cursor.execute(query)
    query_result = cursor.fetchall()
    return jsonify({"timedate" : query_result[0][0], "value1" : query_result[0][1], "value2" : query_result[0][2]})

@app.get("/recent_point")
def get_recent_10_points_of_data():
    query = "select * from generated_data ORDER BY datetime DESC LIMIT 10"
    cursor.execute(query)
    query_result = cursor.fetchall()

    date = []
    value1 = []
    value2 =  []

    for x in query_result:
        date.append(x[0])
        value1.append(x[1])
        value2.append(x[2])

    pd_df = pd.DataFrame({"timedate" : date, "value1" : value1, "value2" : value2}).to_dict()
    return jsonify(pd_df)

@app.get("/all_points")
def get_all_points_of_data():
    query = "select * from generated_data"
    cursor.execute(query)
    query_result = cursor.fetchall()

    date = []
    value1 = []
    value2 =  []

    for x in query_result:
        date.append(x[0])
        value1.append(x[1])
        value2.append(x[2])

    pd_df = pd.DataFrame({"timedate" : date, "value1" : value1, "value2" : value2}).to_dict()
    return jsonify(pd_df)

if __name__ == "__main__":
    app.run(debug=True)