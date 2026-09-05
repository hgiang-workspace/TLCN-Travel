from sklearn.metrics import classification_report, confusion_matrix
import numpy as np
import pandas as pd

def evaluate_predictions(true_labels, pred_labels, label_names=["negative", "neutral", "positive"]):
    print("Classification Report:")
    print(classification_report(true_labels, pred_labels, target_names=label_names))
    
    cm = confusion_matrix(true_labels, pred_labels)
    print("Confusion Matrix:")
    print(cm)
    
    return {
        "report": classification_report(true_labels, pred_labels, target_names=label_names, output_dict=True),
        "confusion_matrix": cm.tolist()
    }