from datetime import datetime
import pika
import os, sys, json
import mysql.connector

# Connecting to Monogdb server and database
def mysql_connect() -> (None):
    try:
        connection = mysql.connector.connect(host="localhost", user="root", password="root", database = "random_database")
        return connection

    except Exception as error:
        print("Error connecting Mongo: ", error)

def mysql_data_rabbitmq_client() -> (None):

    # Connect to rabbitmq server
    credentials = pika.PlainCredentials('operminuser', 'operminpwd')
    parameters = pika.ConnectionParameters(
        'sv-99311', 5672, '/', credentials, heartbeat=180)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue='hello')

    def callback(ch, method, properties, body):
        print("Received ", body)

        rabbitmq_recevied_data = json.loads(body)

        connect_database = mysql_connect()
        cursor = connect_database.cursor()
        query_data_export = "INSERT INTO generated_data (datetime, value1, value2) VALUES (%s, %s, %s)"
        query_values = (rabbitmq_recevied_data["timedate"], rabbitmq_recevied_data["value1"], rabbitmq_recevied_data["value2"])
        cursor.execute(query_data_export, query_values)
        connect_database.commit()

        print(" *** Pushed to the database B ---> B2 ***")
    
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

    # connection.close()

if __name__ == '__main__':
    try:
        mysql_data_rabbitmq_client()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)