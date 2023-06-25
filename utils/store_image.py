from firebase_admin import credentials, initialize_app, storage

from config import FIREBASE_CONF


# Init firebase with your credentials

class FirebaseImageStore:
    def __init__(self):
        self.cred = credentials.Certificate(FIREBASE_CONF["FIREBASE_CREDENTIALS"])
        initialize_app(self.cred, {'storageBucket': FIREBASE_CONF['FIREBASE_STORAGE_BUCKET']})
        # Get a reference to the Firebase Storage bucket
        self.bucket = storage.bucket()

    def upload_image(self, from_image, to_image):
        # Path to the local image file you want to upload
        # Path to the remote location you want to store the image in Firebase Storage
        # remote_image_path = 'images/violence_230516_175206.png'
        # Upload the file to Firebase Storage
        blob = self.bucket.blob(to_image)
        blob.upload_from_filename(from_image)

        print('File {} uploaded to {}.'.format(from_image, to_image))
