from confluent_kafka import Consumer

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

    # if value is None:
    #     value = 0
    # else:
    #     value = msg.value()
        
    kvalue = msg.key().decode("utf-8", "ignore")
    print(f"Harry: {value}")
    
c.close()

