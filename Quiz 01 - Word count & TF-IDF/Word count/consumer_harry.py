from confluent_kafka import Consumer
import time

c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['streams-wordcount-output'])

while True:

    msg = c.poll(0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    
    value = int.from_bytes(msg.value(),"big")
    kvalue = msg.key().decode("utf-8", "ignore")

    if "harry" in kvalue:
        print(f'Harry: {value}')
    else:
        print('Harry: 0')
    
c.close()

