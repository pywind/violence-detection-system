import json

from config import FIRESTORE_PARAMS
from utils.firestore_database import FirestoreData
from utils.kafkaUtils import KafkaHandler
from utils.store_image import FirebaseImageStore

# initialize firebase firestore utilities
fs = FirebaseImageStore()

# initialize elasticsearch utilities
es = FirestoreData()
consumer = KafkaHandler(is_consumer=True)

if __name__ == '__main__':
    for message in consumer.consume_messages():
        # filename = message['saved_filename']
        if message is not None:
            try:
                data = json.loads(message)
                # Upload to Firebase
                filename = data['saved_filename']
                fs.upload_image(from_image=filename, to_image=filename)
                es.add_document(collection_name=FIRESTORE_PARAMS['INDEX_NAME'], data=data)
            except ValueError:
                print("Failed to parse json: {}".format(message))
