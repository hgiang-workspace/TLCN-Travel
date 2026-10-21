---
language:
- vi
metrics:
- accuracy
- f1
tags:
- sentiment-analysis
- social-listening
library_name: transformers
---

# 5CD-ViSoBERT for Vietnamese Sentiment Analysis

<b>YOU ARE TOO BORED AND TIRED OF HAVING TO BUILD A 🇻🇳 VIETNAMESE SENTIMENT ANALYSIS MODEL OVER AND OVER AGAIN?</b>

<b> BOOM! 🤯 NO WORRIES, WE'RE HERE FOR YOU =)) 🔥!</b>

This model is based on our pretrained [5CD-AI/visobert-14gb-corpus](https://huggingface.co/5CD-AI/visobert-14gb-corpus), which has been continuously trained on a 14GB dataset of Vietnamese social content. So it can perform well with many comment sentiments accompanied by emojis 😂👍💬🔥

Our model is fine-tuned on <b>120K Vietnamese sentiment analysis datasets </b>, including comments and reviews from e-commerce platforms, social media, and forums. Our model has been trained on a diverse range of datasets: SA-VLSP2016, AIVIVN-2019, UIT-VSFC, UIT-VSMEC, UIT-ViCTSD, UIT-ViOCD, UIT-ViSFD, Vi-amazon-reviews, Tiki-reviews.

The model will give softmax outputs for three labels.

<b>Labels</b>:

```
0 -> Negative
1 -> Positive
2 -> Neutral
```

## Dataset
Our training dataset. Because of label ambiguity, with UIT-VSMEC, UIT-ViCTSD, VOZ-HSD, we re-label the dataset with Gemini 1.5 Flash API follow the 3 labels. The specific number of samples for each dataset can be seen below: 
<table border="2">
    <tr align="center">
        <th rowspan="2">Dataset</th>
        <th colspan="3">Train</th>
        <th colspan="3">Test</th>
        <th colspan="3">Val</th>
    </tr>
    <tr align="center">
        <th>Neg</th>
        <th>Pos</th>
        <th>Neu</th>
        <th>Neg</th>
        <th>Pos</th>
        <th>Neu</th>
        <th>Neg</th>
        <th>Pos</th>
        <th>Neu</th>
    </tr>
    <tr align="center">
        <td align="left">All-filtered</td>
        <td>62708</td>
        <td>41400</td>
        <td>11593</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>5079</td>
        <td>3724</td>
        <td>638</td>
    </tr>
      <tr align="center">
        <td align="left">SA-VLSP2016</td>
        <td>4759</td>
        <td>4798</td>
        <td>4459</td>
        <td>1180</td>
        <td>1190</td>
        <td>1114</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
    </tr>
      <tr align="center">
        <td align="left">UIT-VSFC </td>
        <td>5325</td>
        <td>5643</td>
        <td>458</td>
        <td>1409</td>
        <td>1590</td>
        <td>167</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
    </tr>
      <tr align="center">
        <td align="left">UIT-VSMEC (Gemini-label)</td>
        <td>3219</td>
        <td>1665</td>
        <td>594</td>
        <td>458</td>
        <td>407</td>
        <td>210</td>
        <td>71</td>
        <td>388</td>
        <td>239</td>
        <td>52</td>
    </tr>
      <tr align="center">
        <td align="left">AIVIVN-2019</td>
        <td>6776</td>
        <td>7879</td>
        <td>-</td>
        <td>4770</td>
        <td>5168</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
    </tr>
      <tr align="center">
        <td align="left">UIT-ViCTSD (Gemini-label)</td>
        <td>3370</td>
        <td>2615</td>
        <td>933</td>
        <td>3370</td>
        <td>2615</td>
        <td>933</td>
        <td>3370</td>
        <td>2615</td>
        <td>933</td>
    </tr>
      <tr align="center">
        <td align="left">UIT-ViHSD</td>
        <td>4162</td>
        <td>19886</td>
        <td>-</td>
        <td>1132</td>
        <td>5548</td>
        <td>-</td>
        <td>482</td>
        <td>2190</td>
        <td>-</td>
    </tr>
      <tr align="center">
        <td align="left">UIT-ViSFD</td>
        <td>2850</td>
        <td>3670</td>
        <td>1266</td>
        <td>827</td>
        <td>1000</td>
        <td>397</td>
        <td>409</td>
        <td>515</td>
        <td>188</td>
    </tr>
      <tr align="center">
        <td align="left">UIT-ViOCD</td>
        <td>2292</td>
        <td>2095</td>
        <td>-</td>
        <td>279</td>
        <td>270</td>
        <td>-</td>
        <td>283</td>
        <td>265</td>
        <td>-</td>
    </tr>
      <tr align="center">
        <td align="left">Tiki-reviews</td>
        <td>20093</td>
        <td>6669</td>
        <td>4698</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
    </tr>
      <tr align="center">
        <td align="left">VOZ-HSD (Gemini-label)</td>
        <td>2676</td>
        <td>1213</td>
        <td>1071</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
    </tr>
      <tr align="center">
        <td align="left">Vietnamese-amazon-polarity</td>
        <td>2559</td>
        <td>2441</td>
        <td>-</td>
        <td>1017</td>
        <td>983</td>
        <td>-</td>
        <td>523</td>
        <td>477</td>
        <td>-</td>
    </tr>
</table>

## Evaluation
<table>
        <tr align="center">
            <td rowspan=2><b>Model</td>
            <td colspan=4><b>SA-VLSP2016</td>
            <td colspan=4><b>AIVIVN-2019</td>
            <td colspan=4><b>UIT-VSFC</td>
            <td colspan=4><b>UIT-VSMEC (Gemini-label)</td>
            <td colspan=4><b>UIT-ViCTSD (Gemini-label)</td>
        </tr>
        <tr align="center">
            <td><b>Acc</td>
            <td><b>Prec</td>
            <td><b>Recall</td>
            <td><b>WF1</td>
            <td><b>Acc</td>
            <td><b>Prec</td>
            <td><b>Recall</td>
            <td><b>WF1</td>
            <td><b>Acc</td>
            <td><b>Prec</td>
            <td><b>Recall</td>
            <td><b>WF1</td>
            <td><b>Acc</td>
            <td><b>Prec</td>
            <td><b>Recall</td>
            <td><b>WF1</td>
            <td><b>Acc</td>
            <td><b>Prec</td>
            <td><b>Recall</td>
            <td><b>WF1</td>
        </tr>
        <tr align="center">
        <tr align="center">
            <td align="left">wonrax/phobert-base-vietnamese-sentiment</td>
            <td>61.65</td>
            <td>63.95</td>
            <td>61.65</td>
            <td>60.01</td>
            <td>84.87</td>
            <td>95.12</td>
            <td>84.87</td>
            <td>89.47</td>
            <td>76.37</td>
            <td>88.10</td>
            <td>76.37</td>
            <td>79.53</td>
            <td>65.41</td>
            <td>74.36</td>
            <td>65.41</td>
            <td>68.33</td>
            <td>62.34</td>
            <td>73.08</td>
            <td>62.34</td>
            <td>65.54</td>
        </tr>
        <tr align="center">
            <td align="left"><b>5CD-AI/Vietnamese-Sentiment-visobert</td>
            <td><b>88.06</td>
            <td><b>88.16</td>
            <td><b>88.06</td>
            <td><b>88.06</td>
            <td><b>99.62</td>
            <td><b>99.65</td>
            <td><b>99.62</td>
            <td><b>99.64</td>
            <td><b>94.65</td>
            <td><b>93.30</td>
            <td><b>93.65</td>
            <td><b>93.38</td>
            <td><b>77.91</td>
            <td><b>77.21</td>
            <td><b>77.91</td>
            <td><b>77.46</td>
            <td><b>75.10</td>
            <td><b>74.59</td>
            <td><b>75.10</td>
            <td><b>74.79</td>
        </tr>
    </div>
</table>


<table>
        <tr align="center">
            <td rowspan=2><b>Model</td>
            <td colspan=4><b>UIT-ViOCD</td>
            <td colspan=4><b>UIT-ViSFD</td>
            <td colspan=4><b>Vi-amazon-polar</td>
        </tr>
        <tr align="center">
            <td><b>Acc</td>
            <td><b>Prec</td>
            <td><b>Recall</td>
            <td><b>WF1</td>
            <td><b>Acc</td>
            <td><b>Prec</td>
            <td><b>Recall</td>
            <td><b>WF1</td>
            <td><b>Acc</td>
            <td><b>Prec</td>
            <td><b>Recall</td>
            <td><b>WF1</td>
        </tr>
        <tr align="center">
        <tr align="center">
            <td align="left">wonrax/phobert-base-vietnamese-sentiment</td>
            <td>74.68</td>
            <td>87.14</td>
            <td>74.68</td>
            <td>78.13</td>
            <td>67.90</td>
            <td>67.95</td>
            <td>67.90</td>
            <td>66.98</td>
            <td>61.40</td>
            <td>76.53</td>
            <td>61.40</td>
            <td>65.70</td>
        </tr>
        <tr align="center">
            <td align="left"><b>5CD-AI/Vietnamese-Sentiment-visobert</td>
            <td><b>94.35</td>
            <td><b>94.74</td>
            <td><b>94.35</td>
            <td><b>94.53</td>
            <td><b>93.26</td>
            <td><b>93.20</td>
            <td><b>93.26</td>
            <td><b>93.21</td>
            <td><b>89.90</td>
            <td><b>90.13</td>
            <td><b>89.90</td>
            <td><b>90.01</td>
        </tr>
    </div>
</table>


## Usage (HuggingFace Transformers)

Install `transformers` package:
    
    pip install transformers


### Pipeline
```python
from transformers import pipeline
model_path = '5CD-AI/Vietnamese-Sentiment-visobert'
sentiment_task = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)
sentiment_task("Miếng dán dễ xước , ko khít với dt 11 prm")
```
Output:
```
[{'label': 'NEG', 'score': 0.998149037361145}]
```

### Other examples
```
Sentence:  Đây là mô hình rất hay, đáp ứng tốt nhu cầu của nhiều doanh nghiệp Việt.
### Sentiment score ####
1) POS: 0.9995
2) NEG: 0.0003
3) NEU: 0.0003
```

```
Sentence:  Qua vụ này thì uy tín của Trump càng lớn hơn nữa. Nhất là với hình ảnh đầy tính biểu tượng như trên.
### Sentiment score ####
1) POS: 0.9965
2) NEG: 0.0029
3) NEU: 0.0005
```

```
Sentence:  Bãi đi nó lừa lắm, mình có bỏ vào ví tt này hơn 20 triệu. Lãi tính ra cả tháng dc bao nhiêu mình không nhớ, nhưng khi rút về ngân hàng nó trừ phí giao dịch hơn mịa nó tiền lãi.
Nên từ đó cạch luôn
### Sentiment score ####
1) NEG: 0.999
2) POS: 0.0008
3) NEU: 0.0002
```

```
Sentence:  Vậy chắc tùy nơi rồi :D
Chỗ mình chuộng hàng masan lắm, mì gói thì không hẳn (có kokomi cũng bán chạy), con gia vị thì gần như toàn đồ masan.
### Sentiment score ####
1) NEU: 0.9824
2) NEG: 0.0157
3) POS: 0.0019
```

```
Sentence:  hội sở ở tech trần duy hưng có 1 thằng là thằng Đạt hói. Làm lâu lên lão làng, đc làm lãnh đạo nhưng chả có cái việc mẹ gì chỉ được ngồi  xếp ca cho nhân viên. xấu tính bẩn tính sân si nhất cái Tech*. Nghiệp vụ thì ậm ờ đ*o biết gì, chỉ suốt ngày nhận lương đi săm soi nhân viên là nhanh =))) đàn ông đàn ang chả khác mẹ gì mấy con mụ ngoài chợ, nó hành từng nhân viên ra bã, trừ đứa nào nịnh nọt ve vãn với nó. Lậy luôn đhs 1 thằng như thế lại được lên làm lead ở Tech.
### Sentiment score ####
1) NEG: 0.9994
2) POS: 0.0006
3) NEU: 0.0001
```

```
Sentence:  Cà phê dở ko ngon, ai chưa mua thì đừng mua
### Sentiment score ####
1) NEG: 0.9994
2) POS: 0.0005
3) NEU: 0.0001
```

```
Sentence:  Cũng tạm. Ko gì đb
### Sentiment score ####
1) NEU: 0.9387
2) NEG: 0.0471
3) POS: 0.0142
```

```
Sentence: thui báo ơi.nhà từ trong trứng ra mà sao sáng đc.
### Sentiment score ####
1) NEG: 0.988
2) POS: 0.0119
3) NEU: 0.0001
```

```
Sentence:  Dm mới kéo cái tuột luôn cái kính cường lực🙂
R phải cầm cái kính tự dán🙂 để lâu quá nó dính hai cục bụi lên nữa chứ má bực thiệt chứ
Hình như tại hai cái cục nam châm nó xúc ra 😑
### Sentiment score ####
1) NEG: 0.9928
2) POS: 0.0071
3) NEU: 0.0001
```

```
Sentence:  Mấy cái khóa kiểu này ông lên youtube tự học còn ngon hơn.
### Sentiment score ####
1) NEG: 0.9896
2) POS: 0.008
3) NEU: 0.0024
```

### Full classification

```python
from transformers import AutoModelForSequenceClassification
from transformers import TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
import numpy as np
import torch

#### Load model
model_path = '5CD-AI/Vietnamese-Sentiment-visobert'
tokenizer = AutoTokenizer.from_pretrained(model_path)
config = AutoConfig.from_pretrained(model_path)
model = AutoModelForSequenceClassification.from_pretrained(model_path).to("cuda")

sentence = 'Cũng giống mấy khoá Youtube học cũng được'
print('Sentence: ', sentence)

input_ids = torch.tensor([tokenizer.encode(sentence)]).to("cuda")

with torch.no_grad():
    out = model(input_ids)
    scores = out.logits.softmax(dim=-1).cpu().numpy()[0]

# Print labels and scores
ranking = np.argsort(scores)
ranking = ranking[::-1]
print("### Sentiment score ####")
for i in range(scores.shape[0]):
    l = config.id2label[ranking[i]]
    s = scores[ranking[i]]
    print(f"{i+1}) {l}: {np.round(float(s), 4)}")
```
Output: 

```
Sentence:  Cũng giống mấy khoá Youtube học cũng được
### Sentiment score ####
1) NEU: 0.8928
2) NEG: 0.0586
3) POS: 0.0486

```


## Fine-tune Configuration
We fine-tune `5CD-AI/visobert-14gb-corpus` on downstream tasks with `transformers` library with the following configuration:
- seed: 42
- gradient_accumulation_steps: 1
- weight_decay: 0.01
- optimizer: AdamW with betas=(0.9, 0.999) and epsilon=1e-08
- training_epochs: 5
- model_max_length: 256
- learning_rate: 2e-5
- metric_for_best_model: wf1
- strategy: epoch
## References
[1] [PhoBERT: Pre-trained language models for Vietnamese](https://aclanthology.org/2020.findings-emnlp.92/)

[2] [ViSoBERT: A Pre-Trained Language Model for Vietnamese Social Media Text Processing](https://aclanthology.org/2023.emnlp-main.315/)

[3] [The Amazon Polarity dataset](https://paperswithcode.com/dataset/amazon-polarity-1)


## Disclaimer
Disclaimer: The data contains actual comments on social networks that might be construed as abusive, offensive, or obscene. Additionally, the examples and dataset may contain negative information about any business. We only collect this data and do not bear any legal responsibility.