import os

spark_nlp_path = 'spark-nlp-5.1.4.tar.gz'
os.system(f"pip install --no-index --find-links=./ {spark_nlp_path}")

import sparknlp
spark = sparknlp.start()

# Import the required modules and classes

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

lemmas_small = "gs://spark_job_b/lemmas_small.txt"
default_sentiment_dict = "gs://spark_job_b/default-sentiment-dict.txt"

# Step 2: Sentence Detection
sentence_detector = SentenceDetector().setInputCols(["document"]).setOutputCol("sentence")
# Step 3: Tokenization
tokenizer = Tokenizer().setInputCols(["sentence"]).setOutputCol("token")
# Step 4: Lemmatization
lemmatizer= Lemmatizer().setInputCols("token").setOutputCol("lemma").setDictionary(lemmas_small, key_delimiter="->", value_delimiter="\t")
# Step 5: Sentiment Detection
sentiment_detector= (
    SentimentDetector()
    .setInputCols(["lemma", "sentence"])
    .setOutputCol("sentiment_score")
    .setDictionary(default_sentiment_dict, ",")
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

# Create a spark Data Frame with an example sentence
data = spark.createDataFrame(
    [
        [
            "The restaurant staff is very bad"
        ]
    ]
).toDF("text") # use the column name `text` defined in the pipeline as input
# Fit-transform to get predictions
result = pipeline.fit(data).transform(data).show(truncate = 50)
