import torch
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, AutoModelForSequenceClassification, AdamW
from datasets import load_dataset
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from tqdm import tqdm
import os

class SentimentDataset(Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        item = {key: val[idx] for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx], dtype=torch.long)
        return item

def main():
    model_name = "vinai/phobert-base"
    
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    
    # Load sample data
    data = [
        {"text": "Hotel rất tốt, staff friendly và vị trí đắc địa", "label": 1},
        {"text": "Phòng sạch sẽ nhưng dịch vụ chậm", "label": 0},
        {"text": "Đặt phòng thất thực, tiêu chuẩn kém", "label": 0},
        {"text": "Tổng体验 tốt, sẽ quay lại lần sau", "label": 1},
        {"text": "Tuyệt vời! Phòng rộng và đẹp", "label": 1},
    ]
    
    texts = [d["text"] for d in data]
    labels = [d["label"] for d in data]
    
    # Split data
    train_texts, val_texts, train_labels, val_labels = train_test_split(
        texts, labels, test_size=0.2, random_state=42
    )
    
    # Tokenize
    train_encodings = tokenizer(
        train_texts, truncation=True, padding=True, max_length=512
    )
    val_encodings = tokenizer(
        val_texts, truncation=True, padding=True, max_length=512
    )
    
    train_dataset = SentimentDataset(train_encodings, train_labels)
    val_dataset = SentimentDataset(val_encodings, val_labels)
    
    train_loader = DataLoader(train_dataset, batch_size=16)
    val_loader = DataLoader(val_dataset, batch_size=16)
    
    model = AutoModelForSequenceClassification.from_pretrained(
        model_name, num_labels=3  # negative, neutral, positive
    )
    
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    
    optimizer = AdamW(model.parameters(), lr=2e-5)
    
    # Training loop
    for epoch in range(3):
        model.train()
        total_loss = 0
        
        for batch in train_loader:
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            labels = batch['labels'].to(device)
            
            outputs = model(
                input_ids=input_ids,
                attention_mask=attention_mask,
                labels=labels
            )
            
            loss = outputs.loss
            total_loss += loss.item()
            
            loss.backward()
            optimizer.step()
            optimizer.zero_grad()
        
        print(f"Epoch {epoch+1}/3, Loss: {total_loss/len(train_loader):.4f}")
    
    # Evaluation
    model.eval()
    correct = 0
    total = 0
    
    with torch.no_grad():
        for batch in val_loader:
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            labels = batch['labels'].to(device)
            
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            predictions = torch.argmax(outputs.logits, dim=1)
            
            total += labels.size(0)
            correct += (predictions == labels).sum().item()
    
    print(f"Validation Accuracy: {correct/total:.4f}")
    
    # Save model
    model.save_pretrained("/opt/nlp/models/visobert/sentiment/v1")
    tokenizer.save_pretrained("/opt/nlp/models/visobert/sentiment/v1")
    
    print("Sentiment model saved!")

if __name__ == "__main__":
    main()