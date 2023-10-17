# Topic Mining 

Topic Mining is an unsupervised machine learning approach that can scan a series of documents, find word and phrase patterns within them, and automatically cluster word groupings and related expressions that best represent them. We leveraged the following models for our analysis:

1. Latent Dirichlet Allocation (LDA) - It is most popular topic modelling technique, is a generative probabilistic model for discrete datasets such as text corpora. It is considered a three-level hierarchical Bayesian model, where each collection item is represented as a finite mixture over an underlying set of topics, and each topic is represented as an infinite mixture over a collection of topic probabilities.

2. Non Matrix Factorization (NMF) - In contrast to LDA, NMF is a decompositional, non-probabilistic algorithm using matrix factorization and belongs to the group of linear-algebraic algorithms. NMF works on TF-IDF transformed data by breaking down a matrix into two lower-ranking matrices, where TF-IDF is a measure to evaluate the importance of a word in a collection of documents. NMF decomposes its input, which is a term-document matrix (A), into a product of a terms-topics matrix (W) and a topics-documents matrix (H).

3. BERTopic - It builds upon the mechanisms of Top2Vec. BERT is used as an embedder, and BERTopic provides document embedding extraction, with a sentence-transformers model for more than 50 languages. BERTopic also supports UMAP for dimension reduction and HDBSCAN for document clustering. The main difference between Top2Vec is the application of a class-based term frequency inverse document frequency (c-TF-IDF) algorithm, which compares the importance of terms within a cluster and creates term representation. This means that the higher the value is for a term, the more representative it is of its topic.

The output of these models is used to infer the topic hierarchy and formulate the regex patterns based on it. The code is organized as follows:
- TopicMining_MainNotebook - It is the main notebook which calls all the necessary functions utilities, from reading the input from the containers to running different topic modelling algorithms and visualizing the outputs .
- TopicMining_Utility.py - It is utility python file which contains all the required functions for extracting the data from the source and running LDA, NMF, and BERTopic. 


#### Overview

1. InputDataReader is a class which is used to read the input data. The input data to this module is the output from the data preprocessor module. It expects parameters such as start_date, end_date, input_path, language and business_unit. The start_date and end_date are used to filter the data for which we want to do the analysis. Currently, these algorithms work for 'English' language only. 

- generate_date_folders() - It generates a list of folder names based on given start date and end date.
- generate_transcripts_df(folder_name) - It generates a dataframe containing the call transcripts for a particular date and is used by union_all_dates_df().
- union_all_dates_df() - It aggregates all the daily transcripts dataframes into one dataframe.

2. TopicMining is a class used to perform Topic Mining leveraging different models (LDA, NMF, and BERTopic). It expects parameters such as model_path, custom_stopwords_file_path, manual_add_stopwords and manual_remove_stopwords. The model_path is the path of required NLP models, stopwords_file_path is the the path where custom stop word files are stored, the stopwords_remove_lst is a list of custom stop words to be removed and the stopwords_add_lst is a list of custom stop words to be added.

- modify_stopwords(stopwords_path, stopwords_remove, stopwords_add) - It modifies the stop words in given path based on the add and remove stop words inputs.
- text_cleaning_lemmatization(text) - It performs the following cleaning steps on the text - removes punctuation, removes white spaces, expands the contractions, removes the accented characters, converts the text to lowercase, remove the digits, removes the the consecutive spaces, removes the leading and trailing spaces, word tokenizes the text and lastly, performs lemmatization.
- generate_WordCloud() - It generates the WordCloud using cleaned and lemmatized transcripts.
- run_Bigram_Trigram(bigram_min_count, trigram_min_count, bigram_threshold, trigram_threshold) - It builds the bigram and trigram models using cleaned transcripts. The input parameters are bigram_min_count which is the minimum count for bigram; trigram_min_count which is the minimum count for trigram; bigram_threhold is the score threshold for the bigram phrases; and trigram_threhold is the score threshold for the trigram phrases.
- run_LDA_model(documents, start_num_topic, end_num_topic, step) - It runs the LDA model using different number of topics. The input parameters to the model are - documents: a list of sentences where each sentence is a list of tokenized word, start_num_topic: the start point of number of topics, end_num_topic: the end point of number of topics and step: the step size between start and end points of number of topics. It returns the corpus, dictionary, model_list, and coherence_values.
- optimal_LDA_visualizaitons(index, model_list, corpus, dictionary) - It takes the output from run_LDA_model() as input and generates the topics and visualizations for the optimal LDA model.
- run_NMF_model_gensim(documents, start_num_topic, end_num_topic, step) - It runs the NMF model using different number of topics. The input parameters to the model are - documents: a list of sentences where each sentence is a list of tokenized word, start_num_topic: the start point of number of topics, end_num_topic: the end point of number of topics and step: the step size between start and end points of number of topics. It returns the corpus, dictionary, model_list, and coherence_values, similar to LDA.
- optimal_LNMF_visualizaitons(index, model_list, corpus, dictionary) - It takes the output from run_NMF_model() as input and generates the topics and visualizations for the optimal NMF model.
- run_NMF_model_sklearn(num_topics, documents) - It runs the NMF model with sklearn taking the tokenized documents and number of topics as input.
- run_BERTopic_model() - It runs the BERTopic model and returns the BERTopic model, the topic number for each transcript, the probability of topic for each transcript, and a dataframe of topics with corresponding keywords.

#### Running the code
Run the topic miner main notebook (/src/Topic_Mining/TopicMining_MainNotebook.py) and provide the following parameters:
- start_date: the start date used to filter data
- end_date: the end date used to filter data
- input_path: the path where the data preprocessor result is stored
- model_path: the path of required NLP models
- custom_stopwords_file_path: the path where custom stop word files are stored
- manual_remove_stopwords: a list of custom stop words to be removed
- manual_add_stopwords: a list of custom stop words to be added
- language: the language by which we want to filter the data preprocessor data (currently it works for English data only)
- business_unit: the business unit for which we want to do the analysis

Once you run the notebook, interpret and infer the topic results manually and formulate different regex patterns based on it. 

##### Sample parameters passed as input
```
{ 
  "start_date": "2022-10-01", 
  "end_date": "2022-10-03", 
  "input_data_path": "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_GB_TM_Output/pre_processor/", 
  "nlp_model_path": "/dbfs/databricks/", "custom_stopwords_file_path": "/dbfs/FileStore/data-01/MFCGD.com/zhuyunq/custom_stop_words/", 
  "manual_remove_stopwords": ["no", "not", "out"], 
  "manual_add_stopwords": ["agent", "customer", "manulife", "benefit", "call", "service", "phone", "thank", "go", "hold", "know", "want"], 
  "language": "English", 
  "business_unit": "GB"
}
```
