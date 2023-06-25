from dataclasses import dataclass


@dataclass(init=True, repr=True)
class DataObject:
    """_summary_
      Class for keeping track of an item DataObject
    Returns:
        _type_: _description_
    """
    predicted_class_name: str
    predicted_labels_probabilities: float
    saved_filename: str
    timestamp: str
    location: dict

    def to_dict(self):
        return {
            "predicted_class_name": self.predicted_class_name,
            "predicted_labels_probabilities": self.predicted_labels_probabilities,
            "saved_filename": self.saved_filename,
            "timestamp": self.timestamp,
            "location": self.location,
        }
