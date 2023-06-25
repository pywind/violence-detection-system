from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


class MongoDB:
    def __init__(self, uri, dbname, collection_name):
        self.client = MongoClient(uri, server_api=ServerApi('1'))
        self.db = self.client[dbname]
        self.collection = self.db[collection_name]

    def get_all_documents(self):
        return list(self.collection.find())

    def delete_all_documents(self):
        self.collection.delete_many({})

    def get_item_by_field(self, field, value):
        return list(self.collection.find({field: value}))

    def add_document(self, document):
        self.collection.insert_one(document)

    def count_documents(self):
        return self.collection.count_documents({})


if __name__ == '__main__':
    # Initialize the MongoDB class with authentication
    uri = "mongodb+srv://piwindy21:0ydJGb6vT6BZBCbH@cluster0.bfxzxxb.mongodb.net/?retryWrites=true&w=majority"

    mongo = MongoDB(uri=uri, dbname="violence_db", collection_name="violence_report")

    # Send a ping to confirm a successful connection
    try:
        # Get all documents in the collection
        all_documents = mongo.get_all_documents()

    except Exception as e:
        print(e)
    """
    
    # Delete all documents in the collection
    mongo.delete_all_documents()
  
    # Get items by field value
    items = mongo.get_item_by_field('field_name', 'field_value')
  
    # Add a new document to the collection
    new_document = {'field_name': 'field_value'}
    mongo.add_document(new_document)
  
    # Count the number of documents in the collection
    num_documents = mongo.count_documents()
    """
