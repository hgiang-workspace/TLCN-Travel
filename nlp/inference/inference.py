from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

def inference(text, model_path="/opt/nlp/models/visobert/sentiment/v1"):
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model = AutoModelForSequenceClassification.from_pretrained(model_path)
    
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    
    with torch.no_grad():
        outputs = model(**inputs)
    
    probabilities = torch.softmax(outputs.logits, dim=1).squeeze().tolist()
    predicted_class = torch.argmax(outputs.logits, dim=1).item()
    
    labels = {0: "negative", 1: "neutral", 2: "positive"}
    
    return {
        "text": text,
        "predicted_label": labels[predicted_class],
        "probabilities": {
            "negative": probabilities[0],
            "neutral": probabilities[1],
            "positive": probabilities[2]
        }
    }

if __name__ == "__main__":
    result = inference("Hotel quá đẹp, staff rất friendly!")
    print(result)