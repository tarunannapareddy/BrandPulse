import sparknlp
spark = sparknlp.start()

# # Import the required modules and classes

from sparknlp.base import DocumentAssembler, Pipeline, Finisher
from sparknlp.annotator import (
    SentenceDetector,
    Tokenizer,
    Lemmatizer,
    SentimentDetector
)
import pyspark.sql.functions as F
# Step 1: Transforms raw texts to `document` annotation
document_assembler = (
    DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")
)
# Step 2: Sentence Detection
sentence_detector = SentenceDetector().setInputCols(["document"]).setOutputCol("sentence")
# Step 3: Tokenization
tokenizer = Tokenizer().setInputCols(["sentence"]).setOutputCol("token")
# Step 4: Lemmatization
lemmatizer= Lemmatizer().setInputCols("token").setOutputCol("lemma").setDictionary("lemmas_small.txt", key_delimiter="->", value_delimiter="\t")
# Step 5: Sentiment Detection
sentiment_detector= (
    SentimentDetector()
    .setInputCols(["lemma", "sentence"])
    .setOutputCol("sentiment_score")
    .setDictionary("default-sentiment-dict.txt", ",")
)
# Step 6: Finisher
finisher= (
    Finisher()
    .setInputCols(["sentiment_score"]).setOutputCols("sentiment")
)
# Define the pipeline
pipeline = Pipeline(
    stages=[
        document_assembler,
        sentence_detector, 
        tokenizer, 
        lemmatizer, 
        sentiment_detector, 
        finisher
    ]
)

data = spark.createDataFrame(
    [
        [
            "It is very expensive and the staff is very poor. They do not value customers"
        ],
        [
            "Quality if maintained. The cleanliness of restaurant is top notch. And the food is delicious"
        ]
    ]
).toDF("text") # use the column name `text` defined in the pipeline as input
# Fit-transform to get predictions
result = pipeline.fit(data).transform(data).show(truncate = 50)