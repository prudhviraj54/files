# Databricks notebook source
# MAGIC %pip install bertopic
# MAGIC %pip install contractions
# MAGIC %pip install unidecode
# MAGIC %pip install wordcloud

# COMMAND ----------

import TopicMining_Utility as topic_mining
import json
import pyLDAvis 
from pprint import pprint

# COMMAND ----------

# Read Parameters
json_string = '{"start_date": "2023-02-10", "end_date": "2023-02-13", "input_data_path": "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/DEV_IIC/pre_processor/", "nlp_model_path": "/dbfs/databricks/", "custom_stopwords_file_path": "/dbfs/FileStore/data-01/MFCGD.com/zhuyunq/custom_stop_words/", "manual_remove_stopwords": ["no", "not", "out"], "manual_add_stopwords": ["agent", "customer", "manulife", "call", "service", "phone", "thank", "go", "hold", "know", "want"], "language": "English", "business_unit": "IIC"}'
json_file = json.loads(json_string)
params = json.loads(json_string)

start_date = params['start_date']
end_date = params['end_date']
input_path = params['input_data_path']
model_path = params['nlp_model_path']
stopwords_custom = params['custom_stopwords_file_path']
stopwords_remove = params['manual_remove_stopwords']
stopwords_add = params['manual_add_stopwords']
language = params['language']
business_unit = params['business_unit']

# COMMAND ----------

# Read Input Data
reader = topic_mining.InputDataReader(start_date, end_date, input_path, language, business_unit)
input_df = reader.union_all_dates_df()

# COMMAND ----------

input_df.count()

# COMMAND ----------

# Generate Topics
tm = topic_mining.TopicMining(input_df, model_path, stopwords_custom, stopwords_remove, stopwords_add)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Word Cloud

# COMMAND ----------

# Plot Word Cloud using cleaned transcripts
wordcloud = tm.generate_WordCloud()
wordcloud.to_image()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LDA

# COMMAND ----------

# Bigram & Trigram
bigram_min_count, trigram_min_count, bigram_threshold, trigram_threshold = 200, 200, 50, 50
docs_bigrams, bigrams_words_count, docs_trigrams, trigrams_words_count = tm.run_Bigram_Trigram(bigram_min_count, trigram_min_count, bigram_threshold, trigram_threshold)
pprint(bigrams_words_count)
pprint(trigrams_words_count)

# COMMAND ----------

# Pick the optimal model
start, end, step = 2, 5, 1
documents = [d.split(' ') for d in tm.docs] # Use original docs or docs_bigrams or docs_trigrams - documents = docs_bigrams / docs_trigrams
lda_corpus, lda_dictionary, lda_model_list, lda_coherence_values = tm.run_LDA_model(documents, start, end, step)  

# COMMAND ----------

# Topics for optimal model
index = 1
optimal_lda, vis_lda, df_lda_topics = tm.optimal_LDA_visualizaitons(index, lda_model_list, lda_corpus, lda_dictionary)

# COMMAND ----------

df_lda_topics

# COMMAND ----------

pyLDAvis.enable_notebook()
vis_lda

# COMMAND ----------

# MAGIC %md
# MAGIC ### NMF - gensim

# COMMAND ----------

# Pick the optimal model
start, end, step = 2, 5, 1
documents = [d.split(' ') for d in tm.docs] # Use original docs or docs_bigrams or docs_trigrams - documents = docs_bigrams / docs_trigrams
nmf_corpus, nmf_dictionary, nmf_model_list, nmf_coherence_values = tm.run_NMF_model_gensim(documents, start, end, step)

# COMMAND ----------

# Topics for optimal model
index = 1
optimal_nmf, df_nmf_topics = tm.optimal_NMF_visualizaitons(index, nmf_model_list, nmf_corpus, nmf_dictionary)

# COMMAND ----------

df_nmf_topics

# COMMAND ----------

# MAGIC %md
# MAGIC ### NMF - sklearn

# COMMAND ----------

num_topics = 3
NMF_model, df_nmf_topics = tm.run_NMF_model_sklearn(num_topics, tm.docs)
df_nmf_topics

# COMMAND ----------

# MAGIC %md
# MAGIC ### BERTopic

# COMMAND ----------

bert_model, topics, probs, df_bert_topics = tm.run_BERTopic_model()
df_bert_topics

# COMMAND ----------

bert_model.visualize_topics()

# COMMAND ----------

bert_model.visualize_barchart()

# COMMAND ----------
