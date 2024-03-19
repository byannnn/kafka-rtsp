import threading
from confluent_kafka import Consumer, KafkaError, KafkaException

import cv2
import numpy as np
import time
import os

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'kafka-multi-video-stream',
    'enable.auto.commit': False,
    'default.topic.config': {'auto.offset.reset': 'earliest'}
}

class ConsumerThread:
    def __init__(self, config, topic):
        self.config = config
        self.topic = topic

    def read_data(self):
        consumer = Consumer(self.config)
        consumer.subscribe(self.topic)
        self.run(consumer, 0, [], [])

    def run(self, consumer, msg_count, msg_array, metadata_array):
        try:
            while True:
                msg = consumer.poll(0.5)
                if msg == None:
                    continue
                elif msg.error() == None:

                    # convert image bytes data to numpy array of dtype uint8
                    nparr = np.frombuffer(msg.value(), np.uint8)

                    # decode image
                    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                    msg_array.append(img)

                    # get metadata
                    frame_no = msg.timestamp()[1]
                    stream_name = msg.headers()[0][1].decode("utf-8")

                    metadata_array.append((frame_no, stream_name))

                    try:
                        os.makedirs(f"frames/{stream_name}/")
                    except Exception as e:
                        pass
                    
                    cv2.imwrite(f"frames/{stream_name}/{frame_no}.jpg", img)

                    print(f"Wrote frame {frame_no}")

                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached {0}/{1}'
                        .format(msg.topic(), msg.partition()))
                else:
                    print('Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            print("Detected Keyboard Interrupt. Quitting...")
            pass

        finally:
            consumer.close()

    def start(self, numThreads):
        # Note that number of consumers in a group shouldn't exceed the number of partitions in the topic
        for _ in range(numThreads):
            t = threading.Thread(target=self.read_data)
            t.daemon = True
            t.start()
            while True: time.sleep(10)

if __name__ == "__main__":
    topic = ["multi-video-stream"]
    
    consumer_thread = ConsumerThread(config, topic)
    consumer_thread.start(3)