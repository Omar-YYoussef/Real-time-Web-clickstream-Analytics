from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import json
import csv

# Kafka broker configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'topic': 'clickstreamV1', # Topic to which data will be published
}

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_config['bootstrap.servers'])

# Watchdog event handler to handle file events
class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        self.process_file(event.src_path)

    # def on_modified(self, event):
    #     self.process_file(event.src_path)

    def process_file(self, file_path):
        # Check if the file is a JSON file
        if file_path.endswith('.json'):
            with open(file_path, 'r') as file:
                data = json.load(file)
                # Process JSON data and send to Kafka topic
                producer.send(kafka_config['topic'], value=json.dumps(data).encode())

        # Check if the file is a CSV file
        elif file_path.endswith('.csv'):
            with open(file_path, 'r') as file:
                # Process CSV data and send each row to Kafka topic
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    producer.send(kafka_config['topic'], value=json.dumps(row).encode())

# Watchdog observer to monitor the directory for file events
observer = Observer()
event_handler = FileEventHandler()
directory_to_watch = '../../data/data_stream/'  # Replace with your directory path

observer.schedule(event_handler, directory_to_watch)
observer.start()

try:
    while True:
        pass  # Keep running until interrupted
except KeyboardInterrupt:
    observer.stop()

observer.join()