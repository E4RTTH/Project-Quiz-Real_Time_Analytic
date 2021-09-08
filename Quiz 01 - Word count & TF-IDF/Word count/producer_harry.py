import time
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

with open('Harry Potter.txt','r',encoding="utf-8") as f:
    read = f.readlines()

file_source = "C:\my_config\/file-source.txt"

for i in range(7,100):
    if read[i] != "\n":
        p.poll(0)
        p.produce('streams-plaintext-input', read[i].encode('utf-8'), callback=delivery_report)
        time.sleep(1)

p.flush()