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

    def publishFrame(self, video_path):
        cap = cv2.VideoCapture(video_path)
        video_name = os.path.basename(video_path).split(".")[0]
        frame_no = 1
        while cap.isOpened():
            _, frame = cap.read()
            # pushing every 3rd frame
            if frame_no % 3 == 0:
                _, img_buffer_arr = cv2.imencode(".jpg", frame)
                frame_bytes = img_buffer_arr.tobytes()

                self.producer.produce(
                    topic="multi-video-stream", 
                    value=frame_bytes, 
                    on_delivery=delivery_report,
                    timestamp=frame_no,
                    headers={
                        "video_name": str.encode(video_name)
                    }
                )
            frame_no += 1
        cap.release()
        return
        
    def start(self, vid_paths):
        # runs until the processes in all the threads are finished
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.publishFrame, vid_paths)

        self.producer.flush() # push all the remaining messages in the queue

if __name__ == "__main__":
    video_paths = [
        "rtsp://laurettatraining:8554/172-16-17-55",
        "rtsp://laurettatraining:8554/172-16-17-77",
        "rtsp://laurettatraining:8554/172-16-17-96",
        "rtsp://laurettatraining:8554/172-16-17-132",
    ]

    producer_thread = ProducerThread()
    producer_thread.start(video_paths)
    