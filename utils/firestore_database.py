import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter


class FirestoreData:

    def __init__(self) -> None:
        self.__cred = credentials.Certificate("security/distributed-system.json")
        if not firebase_admin._apps:
            self.fs = firebase_admin.initialize_app(self.__cred)
        else:
            self.fs = firebase_admin.get_app()

        self.db = firestore.client()
        # self.collection = self.db.collection(index)

    def get_all(self, collection_name):
        docs = self.db.collection(collection_name).get()
        return docs

    def delete_all_collection(self, collection_name):
        return self.db.collection(collection_name).delete()

    def get_item_by_key_value(self, collection_name, field, value):
        doc = self.db.collection(collection_name).where(filter=FieldFilter(field, ">=", value)).get()
        return doc

    def add_document(self, data, collection_name):
        doc_ref = self.db.collection(collection_name).document()
        doc_ref.set(data)

    def count_all(self, collection_name):
        return len(self.db.collection(collection_name).get())


if __name__ == '__main__':
    fs = FirestoreData()
    a = fs.count_all(collection_name='violence-data')
    print(a)
