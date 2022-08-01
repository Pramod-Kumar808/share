
import pika
import time, datetime, random, json, sys, os,traceback

# Connect to rabbitmq server
credentials = pika.PlainCredentials('operminuser', 'operminpwd')
parameters = pika.ConnectionParameters(
    'sv-99311', 5672, '/', credentials, heartbeat=180)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()


def generate_data_rabbitmq_server(delay : int) -> (None):
    count = 1
    next_time = time.time() + delay
    while True:
        time.sleep(max(0, next_time - time.time()))
        try:
            current_datetime = datetime.datetime.now()
            current_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            data1 = round(random.uniform(0, 10), 2)
            data2 = round(random.uniform(0, 10), 2)
        except Exception:
            traceback.print_exc()

        publish_message_circle = {"timedate": current_datetime, "value1" : data1, "value2" : data2}
        print(publish_message_circle)

        channel.queue_declare(queue='hello')
        channel.basic_publish(exchange='', routing_key='hello', body=json.dumps(publish_message_circle))

        next_time += (time.time() - next_time) // delay * delay + delay
        
        count += 1
        if count == 10:
            break

        print(" *** The result sent to the RabbitMQ client  Task A ---> B *** ")

        # connection.close()




if __name__ == '__main__':
    try:
        generate_data_rabbitmq_server(1)
    except KeyboardInterrupt:
        print('Interrupted by press keyboard key')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
