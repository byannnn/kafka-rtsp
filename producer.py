import concurrent.futures
from confluent_kafka import Producer
import os
import cv2

import logging

# logging.warning('This will get logged to a file')
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

config = {
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,
    'acks': 'all',
    'retries': 100,
    'max.in.flight.requests.per.connection': 5,
    'compression.type': 'snappy',
    'linger.ms': 5,
    'batch.num.messages': 32
}

def delivery_report(err, msg):
    if err:
        logging.error("Failed to deliver message: {0}: {1}".format(msg.value(), err.str()))
    else:
        logging.info(f"msg produced. \n"
                    f"Topic: {msg.topic()} \n" +
                    f"Partition: {msg.partition()} \n" +
                    f"Offset: {msg.offset()} \n" +
                    f"Timestamp: {msg.timestamp()} \n")

class ProducerThread:
    def __init__(self):
        self.producer = Producer(config)

    def publish(self, rtsp_stream):
        cap = cv2.VideoCapture(rtsp_stream)
        stream_name = rtsp_stream.split("8554/")[1]
        frame_no = 1
        while cap.isOpened():
            _, frame = cap.read()
            # pushing every 3rd frame
            if frame_no % 5 == 0:
                _, img_buffer_arr = cv2.imencode(".jpg", frame)
                frame_bytes = img_buffer_arr.tobytes()

                self.producer.produce(
                    topic="multi-video-stream", 
                    value=frame_bytes, 
                    on_delivery=delivery_report,
                    timestamp=frame_no,
                    headers={
                        "stream_name": str.encode(stream_name)
                    }
                )
            frame_no += 1
        cap.release()
        return
        
    def start(self, rtsp_streams):
        # runs until the processes in all the threads are finished
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.publish, rtsp_streams)

        self.producer.flush() # push all the remaining messages in the queue

if __name__ == "__main__":
    rtsp_streams = [
        "rtsp://laurettatraining:8554/172-16-17-55",
        "rtsp://laurettatraining:8554/172-16-17-77",
        "rtsp://laurettatraining:8554/172-16-17-96",
        "rtsp://laurettatraining:8554/172-16-17-132",
    ]

    producer_thread = ProducerThread()
    producer_thread.start(rtsp_streams)
    