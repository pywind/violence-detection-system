import datetime

from numpy import random

records = []
import json

from random import randrange
from datetime import timedelta


def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


start_date = datetime.datetime.strptime('2023/04/21 1:12:13', '%Y/%m/%d %H:%M:%S')
end_date = datetime.datetime.strptime('2023/06/24 23:12:13', '%Y/%m/%d %H:%M:%S')

random.seed(1)

lat = [round(random.uniform(9.9, 11), 6) for i in range(6)]
lon = [round(random.uniform(105.8, 108), 6) for i in range(6)]

for i in range(100):
    timestamp = random_date(start_date, end_date)
    index = random.randint(0, 6)
    record = {
        "predicted_class_name": "True",
        "predicted_labels_probabilities": round(random.uniform(0.85, 0.99), 8),
        "timestamp": timestamp.strftime("%Y/%m/%d %H:%M:%S"),
        "saved_filename": f"violence_{timestamp.strftime('%Y%m%d_%H%M%S')}.png",
        "location": {
            "lat": lat[index],
            "lon": lon[index]
        }
    }
    records.append(record)

with open('data1.json', 'w') as f:
    json.dump(records, f, indent=2)
