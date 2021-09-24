from confluent_kafka import Producer
import time

# p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292'})
p = Producer({'bootstrap.servers': 'ec2-13-229-46-113.ap-southeast-1.compute.amazonaws.com:9092'}) # AWS AJ.POK

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        print('Message delivered to {}'.format(msg.value().decode('utf-8')))


#### SET INPUT TOPIC #####
# set_input_topic = 'streams-plaintext-input'
set_input_topic = 'harry-potter-kvs'

print("Start producer...")

### READ ALL FILE -- HARRY #########
file1 = open('Harry Potter.txt', 'r', encoding = 'utf-8')
Lines = file1.read()
####################################

Lines = Lines.replace("\n", " ")
wholeBook = Lines.split('|')

run_page = 1
for sentence in wholeBook[0:-1]:
    run_page += 1
    page = 'page' + str(run_page)
    p.poll(0)
    kMsg = page.encode().decode('utf-8')
    sendMsg = sentence.encode().decode('utf-8')
    # sendMsg = sentence.encode().decode('utf-8').strip('\n')
    p.produce(set_input_topic, key=kMsg, value=sendMsg , callback=delivery_report)
    print(kMsg,":",sendMsg)

p.flush()