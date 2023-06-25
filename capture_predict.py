#
import datetime
from collections import deque
from typing import List

import cv2
import numpy as np
import tensorflow as tf

from utils.DataObject import DataObject
from utils.kafkaUtils import KafkaHandler

# Capture video from webcam
# Detect violence in the video using a CNN model
# If violence is detected, save the frame, upload to Firebase and publish a message to Kafka
# Initialize the producer
producer_obj = KafkaHandler(is_producer=True)

# Constants
DEQUE_MAXLEN = 15  # Number of frames to check for violence
MODEL_PATH = "model/mobileNetModel.h5"  # Path to CNN model
OUTPUT_DIR = "images/"  # Directory to save images
BASENAME = "violence"  # Basename for saved images
CLASSES_LIST: List[str] = ['NonViolence', 'Violence']  # initialize classes list
TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M:%S"  # Init timestamp format
# Initialize utils

# import tensorflow model from model.h5
frame_queue = deque(maxlen=DEQUE_MAXLEN)  # queue to hold 15 frames
final_model = tf.keras.models.load_model(
    MODEL_PATH, compile=False)  # CNN + LSTM model

# Set default location
location = {"lat": "10.8519702", "lon": "106.7687052,17"}

# Set FPS
INTERVAL = 1000.0 / 3.0


def main():
    # Capture frames
    cap = cv2.VideoCapture(0, cv2.CAP_DSHOW)
    timer = cv2.getTickCount()
    while True:
        # Capture frame at 3 FPS
        if (cv2.getTickCount() - timer) / cv2.getTickFrequency() * 1000 > INTERVAL:
            ret, frame = cap.read()
            if ret:
                # Add the frame to the queue
                # Resize the Frame to fixed Dimensions.
                resized_frame = cv2.resize(frame, (224, 224))
                # Normalize the resized frame.
                normalized_frame = resized_frame / 255.0  # type: ignore
                frame_queue.append(normalized_frame)
                # If queue has 15 frames, make a prediction
                if len(frame_queue) == 15:
                    predicted_labels_probabilities, predicted_class_name = detect_violence(
                        frame_queue)
                    print(
                        f"Predicted: {predicted_class_name}\nConfidence: {predicted_labels_probabilities}"
                    )

                    if predicted_class_name == "Violence":
                        timestamp = datetime.datetime.now().strftime(TIMESTAMP_FORMAT)

                        save_and_upload(frame, predicted_class_name,
                                        predicted_labels_probabilities, timestamp)
                    frame_queue.clear()

            # Restart timer
            timer = cv2.getTickCount()
            cv2.imshow("video", frame)
        if cv2.waitKey(1) & 0xFF == ord("q"):
            break


def detect_violence(frames):
    """Pass frames to CNN + LSTM model and detect violence."""
    # Passing the  pre-processed frames to the model and get the predicted probabilities.
    predicted_labels_probabilities = final_model.predict(  # type: ignore
        np.expand_dims(frames, axis=0), verbose=0)[0]
    # Get the index of class with the highest probability.
    predicted_label = np.argmax(predicted_labels_probabilities)

    # Get the class name using the retrieved index.
    predicted_class_name = CLASSES_LIST[predicted_label]
    probabilities = predicted_labels_probabilities[predicted_label]
    return probabilities, predicted_class_name


def save_and_upload(frame, predicted_class, predicted_probability, timestamp):
    """Save frame, upload to Firebase and publish message.
  
    Args:
        frame: frame prediction
        predicted_class: Class prediction
        predicted_probability: probability (eg 0.)
        timestamp: time action
    """
    # Add label to frame
    cv2.putText(frame, predicted_class, (5, 100),
                cv2.FONT_HERSHEY_SIMPLEX, 3, (0, 0, 255), 12)
    # Convert to new format
    # Generate random filename
    filename = OUTPUT_DIR + \
               '_'.join([BASENAME, generate_magic_string_name(timestamp)]) + ".png"

    # Save image
    cv2.imwrite(filename, frame)

    # Publish message to Kafka
    msg = DataObject(predicted_class_name=predicted_class,
                     predicted_labels_probabilities=predicted_probability,
                     saved_filename=filename,
                     timestamp=timestamp,
                     location=location)

    # producer messages
    producer_obj.produce_message(str(msg.to_dict()))


def generate_magic_string_name(timestamp) -> str:
    """Generate the specified magic string"""
    return datetime.datetime.strptime(timestamp, TIMESTAMP_FORMAT).strftime('%Y%m%d_%H%M%S')


if __name__ == "__main__":
    main()
