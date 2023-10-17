import os
from datetime import datetime, timedelta
import pandas as pd
from collections import Counter
import re
import contractions
import unidecode
import string

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import spacy
from wordcloud import WordCloud
import gensim
from gensim.corpora import Dictionary 
import pyLDAvis.gensim_models
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.decomposition import NMF
from sklearn.preprocessing import normalize
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer

import matplotlib.pyplot as plt

import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

import warnings
warnings.filterwarnings("ignore")


class InputDataReader:
    """
    Class to read the input data
    
    :param str start_date: the start date used to filter data
    :param str end_date: the end date used to filter data
    :param str input_path: the path of raw input data
    :param str language: the language code required
    :param str business_unit: the business unit code required
    """
    
    def __init__(self, start_date, end_date, input_path, language, business_unit):
        self.start_date = start_date
        self.end_date = end_date
        self.input_path = input_path
        self.language = language
        self.bu = business_unit
    
    def generate_date_folders(self):
        """
        Generate a list of folder names based on given start date and end date
        
        :return: a list of string representing date folders
        :rtype: list[str]
        """
        sdate = datetime.strptime(self.start_date, "%Y-%m-%d")
        edate = datetime.strptime(self.end_date, "%Y-%m-%d")
        dates = [sdate + timedelta(days=x) for x in range((edate - sdate).days + 1)]
        folders = [d.strftime("%Y%m%d/") for d in dates]
        
        return folders
    
    def generate_transcripts_df(self, folder_name):
        """
        Generate a dataframe of call transcripts for one date
        
        :param str folder_name: the folder name for the date
        :return: a dataframe containing AWS call transcripts related attributes for the date
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        
        def dialogue_text(x):
            """
            Extract transcript texts in dialogue form
            
            :param dict[str, str] x: a list of dictionary where each dictionary represents a sentence
            :return: a string containing call transcripts in dialogue form
            :rtype: str
            """
            
            sentence_lst = [ s['Caller_Type'] + ': ' + s['Text'] for s in x ]
            text = ' '.join(sentence_lst)
            
            return text
        
        dialogue_text_udf = f.udf(lambda x: dialogue_text(x), StringType())
        
        try:
            df = spark.read.format('parquet').load(self.input_path + folder_name)
            df_filtered = df.filter((f.col('Language') == self.language) & (f.col('BU') == self.bu)).select('Contact_ID', 'Conversations')
            df_filtered = df_filtered.withColumn('Dialogue_Text', dialogue_text_udf(f.col('Conversations'))).drop('Conversations')
            print('Successfully read date folder ' + folder_name)
            return df_filtered
        
        except:
            print('The date folder ' + folder_name + ' does not exist!')
            return spark.createDataFrame([], StructType([StructField('Contact_ID', StringType(), False), StructField('Dialogue_Text', StringType(), True)]))
        
    def union_all_dates_df(self):
        """
        Union daily transcripts dataframes into one dataframe
        
        :return: a dataframe containing AWS call transcripts related attributes from start date to end date
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        folders = self.generate_date_folders()
        df_all_dates = spark.createDataFrame([], StructType([StructField('Contact_ID', StringType(), False),  StructField('Dialogue_Text', StringType(), True)]))
        
        for f in folders:
            df_temp = self.generate_transcripts_df(f)
            df_all_dates = df_all_dates.unionByName(df_temp)
        
        return df_all_dates

    
class TopicMining:
    """
    Class to perform Topic Mining using different models
    
    :param pyspark.sql.dataframe.DataFrame df: a dataframe containing cleaned call transcripts
    :param str model_path: the path of required NLP models
    :param str stopwords_file_path: the path where custom stop word files are stored
    :param list[str] stopwords_remove_lst: a list of custom stop words to be removed
    :param list[str] stopwords_add_lst: a list of custom stop words to be added
    """
    
    def __init__(self, df, model_path, stopwords_file_path, stopwords_remove_lst, stopwords_add_lst):
        self.df = df
        self.docs = None
        self.model_path = model_path
        self.stopwords_lst = list(stopwords.words("english"))
        self.modify_stopwords(stopwords_file_path, stopwords_remove_lst, stopwords_add_lst)
        self.apply_text_cleaning_lemmatization()
        
    def modify_stopwords(self, stopwords_path, stopwords_remove, stopwords_add):
        """
        Modify stop words based on given custom stop words inputs
        
        :param str stopwords_path: the path where custom stop word files are stored
        :param list[str] stopwords_remove: a list of custom stop words to be removed
        :param list[str] stopwords_add: a list of custom stop words to be added
        """
        if stopwords_path != '':
            files_to_add = os.listdir(stopwords_path)
            for f in files_to_add:
                file = pd.read_excel(stopwords_path + f)
                custom_stopwords = file['Column'].tolist()
                self.stopwords_lst.extend(custom_stopwords)
        
        if stopwords_remove != []:
            for w in stopwords_remove:
                self.stopwords_lst.remove(w)
        
        if stopwords_add != []:
            self.stopwords_lst.extend(stopwords_add)
        
        self.stopwords_lst = list(set(self.stopwords_lst))       
        
    def apply_text_cleaning_lemmatization(self):
        """Apply text cleaning and lemmatization on transcripts"""
        stopwords_set = self.stopwords_lst
        nlp = spacy.load(self.model_path + 'Spacy/en_core_web_sm/en_core_web_sm-3.4.0')
        
        def text_cleaning_lemmatization(text):
            """
            Text cleaning and lemmatization
            
            :param str text: the original transcripts
            :return: cleaned and lemmatized transcripts
            :rtype: str
            """
            remove_punc_trans = str.maketrans('', '', string.punctuation)
            text = text.translate(remove_punc_trans)                                # Remove punctuation
            text = contractions.fix(text)                                           # Expand shortened words
            text = unidecode.unidecode(text)                                        # Remove accented characters
            text = re.sub(r'\d+', '', text)                                         # Remove digits
            text = re.sub(' +', ' ', text)                                          # Remove consecutive spaces
            text = ' '.join(text.split())                                           # Remove white spaces
            text = text.strip()                                                     # Remove leading and trailing spaces
            text = text.lower()                                                     # Lowercase
            word_lst = word_tokenize(text)                                          # Tokenize
            sentence = ' '.join(word_lst) 
            token_lst = nlp(sentence)                                               # spacy_lemmatization 
            lemmas = [token.lemma_ for token in token_lst 
                      if token.pos_ in ['NOUN', 'ADJ', 'VERB', 'ADV']]
            filtered_lemmas = [w for w in lemmas if w not in stopwords_set]         # Remove stopwords
            text_cleaned = ' '.join(filtered_lemmas)
            
            return text_cleaned       
        
        text_udf = f.udf(lambda x: text_cleaning_lemmatization(x), StringType())
        
        self.df = self.df.withColumn('TM_Text', text_udf(f.col('Dialogue_Text')))
        self.docs = self.df.select('TM_Text').rdd.flatMap(lambda x: x).collect()
    
    def generate_WordCloud(self):
        """
        Generate WordCloud using cleaned and lemmatized transcripts
        
        :return: a word could built from cleaned and lemmatized transcripts
        :rtype: WordCloud
        """
        long_string = ', '.join(self.docs)
        wordcloud = WordCloud(background_color="white", max_words=5000, contour_width=0, contour_color='steelblue', width=600, height=450)
        wordcloud.generate(long_string)
        
        return wordcloud

    def run_Bigram_Trigram(self, bigram_min_count, trigram_min_count, bigram_threshold, trigram_threshold):
        """
        Build bigram and trigram models using cleaned transcripts
        
        :param int bigram_min_count: the minimum count required for bigram
        :param int trigram_min_count: the minimum count required for trigram
        :param int bigram_threshold: a score threshold for forming the bigram phrases (higher means fewer phrases)
        :param int trigram_threshold: a score threshold for forming the trigram phrases (higher means fewer phrases)
        :returns: 
            - list[list[str]] docs_bigrams - transcript sentences of tokenized words with bigrams
            - Counter bigrams_words_count - counter of bigram words
            - list[list[str]] docs_trigrams - transcript sentences of tokenized words with trigrams
            - Counter trigrams_words_count - counter of trigram words
        """
        words = [d.split(' ') for d in self.docs]
        
        # Bigram
        bigram = gensim.models.Phrases(words, min_count=bigram_min_count, threshold=bigram_threshold)
        bigram_model = gensim.models.phrases.Phraser(bigram)
        docs_bigrams = [bigram_model[doc] for doc in words]
        bigrams_words_count = Counter([w for d in docs_bigrams for w in d if '_' in w])
        
        # Trigram
        trigram = gensim.models.Phrases(bigram[words], min_count=trigram_min_count, threshold=trigram_threshold)  
        trigram_model = gensim.models.phrases.Phraser(trigram)
        docs_trigrams = [trigram_model[bigram_model[doc]] for doc in words]
        trigrams_words_count = Counter([w for d in docs_trigrams for w in d if '_' in w])
        
        return docs_bigrams, bigrams_words_count, docs_trigrams, trigrams_words_count
    
    def run_LDA_model(self, documents, start_num_topic, end_num_topic, step):
        """
        Run series of LDA model using different number of topics
        
        :param list[list[str]] documents: a list of sentences where each sentence is a list of tokenized word
        :param int start_num_topic: the start point of number of topics
        :param int end_num_topic: the end point of number of topics
        :param int step: the step size between start and end points of number of topics
        :returns: 
            - list[list[tuple]] corpus - Term Document Frequency for each tokenized word
            - gensim.corpora.dictionary.Dictionary dictionary - 
            - list[gensim.models.ldamodel.LdaModel] model_list - a list of LDA models with different number of topics
            - list[float] coherence_values - a list of model coherence scores 
        """
        # create Dictionary
        dictionary = Dictionary(documents)
        dictionary.filter_extremes(no_below=2, no_above=0.9, keep_n=10000)
        
        # Term Document Frequency
        corpus = [dictionary.doc2bow(text) for text in documents]
        
        # run LDA with different number of topics
        coherence_values = []
        model_list = []
        for i in range(start_num_topic, end_num_topic+1, step):
            model = gensim.models.ldamodel.LdaModel(corpus=corpus, id2word=dictionary, num_topics=i, update_every=1, passes=10, alpha='auto', random_state=1111)
            model_list.append(model)  
            coherencemodel = gensim.models.CoherenceModel(model=model, texts=documents, dictionary=dictionary, coherence='c_v')
            coherence_values.append(coherencemodel.get_coherence())
        
        # plot Coherence Score - lower the better
        x = range(start_num_topic, end_num_topic+1, step)
        plt.plot(x, coherence_values)
        plt.xlabel('Number of Topics')
        plt.ylabel('Coherence Score')
        plt.show()
        
        # print topics with coherence
        for ind, num_topic, cv in zip(range(len(model_list)), x, coherence_values):
            print('Index', ind, '- Numer of Topics =', num_topic, 'with Coherence Value', round(cv, 4))

        return corpus, dictionary, model_list, coherence_values

    def optimal_LDA_visualizaitons(self, index, model_list, corpus, dictionary):
        """
        Generate topics and visualizations for the optimal LDA model
        
        :param int index: the index of the optimal LDA model
        :param list[gensim.models.ldamodel.LdaModel] model_list: a list of LDA models with different number of topics
        :param list[list[tuple]] corpus: Term Document Frequency for each tokenized word
        :param gensim.corpora.dictionary.Dictionary dictionary: 
        :returns: 
            - gensim.models.ldamodel.LdaModel optimal_lda - the optimal LDA model
            - pyLDAvis._prepare.PreparedData vis -
            - pd.DataFrame df_lda_topics - a dataframe of topics with corresponding keywords
        """
        optimal_lda = model_list[index]
        vis = pyLDAvis.gensim_models.prepare(optimal_lda, corpus, dictionary)
        df_lda_topics = pd.DataFrame()
        
        for ind, topic in optimal_lda.print_topics():
            print("Topic {} - {}".format(ind + 1, topic))
            wp = optimal_lda.show_topic(ind)
            df_lda_topics['Topic_' + str(ind + 1)] = [word for word, prop in wp]
        
        return optimal_lda, vis, df_lda_topics
    
    def run_NMF_model_gensim(self, documents, start_num_topic, end_num_topic, step):
        """
        Run series of NMF model using different number of topics with gensim
        
        :param list[list[str]] documents: a list of sentences where each sentence is a list of tokenized word
        :param int start_num_topic: the start point of number of topics
        :param int end_num_topic: the end point of number of topics
        :param int step: the step size between start and end points of number of topics
        :returns: 
            - list[list[tuple]] corpus - Term Document Frequency for each tokenized word
            - gensim.corpora.dictionary.Dictionary dictionary -  
            - list[gensim.models.nmf.Nmf] model_list - a list of NMF models with different number of topics
            - list[float] coherence_values - a list of model coherence scores 
        """
        # create Dictionary
        dictionary = Dictionary(documents)
        dictionary.filter_extremes(no_below=2, no_above=0.9, keep_n=10000)
        
        # Term Document Frequency
        corpus = [dictionary.doc2bow(text) for text in documents]
        
        # run NMF with different number of topics
        coherence_values = []
        model_list = []
        for i in range(start_num_topic, end_num_topic+1, step):
            model = gensim.models.Nmf(corpus=corpus, id2word=dictionary, num_topics=i, 
                                      chunksize=2000, passes=10, kappa=.1, minimum_probability=0.01, w_max_iter=300, w_stop_condition=0.0001, 
                                      h_max_iter=100, h_stop_condition=0.001, eval_every=10, normalize=True, random_state=11111)
            model_list.append(model)
            coherencemodel = gensim.models.CoherenceModel(model=model, texts=documents, dictionary=dictionary, coherence='c_v')
            coherence_values.append(coherencemodel.get_coherence())
        
        # plot Coherence Score - lower the better
        x = range(start_num_topic, end_num_topic+1, step)
        plt.plot(x, coherence_values)
        plt.xlabel('Number of Topics')
        plt.ylabel('Coherence Score')
        plt.show()

        # print topics with coherence
        for ind, num_topic, cv in zip(range(len(model_list)), x, coherence_values):
            print('Index', ind, '- Numer of Topics =', num_topic, 'with Coherence Value', round(cv, 4))

        return corpus, dictionary, model_list, coherence_values

    def optimal_NMF_visualizaitons(self, index, model_list):
        """
        Generate topics and visualizations for the optimal NMF model
        
        :param int index: the index of the optimal NMF model
        :param list[gensim.models.nmf.Nmf] model_list: a list of NMF models with different number of topics
        :returns: 
            - gensim.models.nmf.Nmf optimal_nmf - the optimal NMF model
            - pd.DataFrame df_nmf_topics - a dataframe of topics with corresponding keywords
        """
        optimal_nmf = model_list[index]
        df_nmf_topics = pd.DataFrame()
        
        for ind, topic in optimal_nmf.print_topics():
            print("Topic {} - {}".format(ind + 1, topic))
            wp = optimal_nmf.show_topic(ind)
            df_nmf_topics['Topic_' + str(ind + 1)] = [word for word, prop in wp]
        
        return optimal_nmf, df_nmf_topics
    
    def run_NMF_model_sklearn(self, num_topics, documents):
        """
        Run NMF model with sklearn
        
        :param int num_topics: the number of topics
        :param list[list[str]] documents: a list of sentences where each sentence is a list of tokenized word
        :returns: 
            - sklearn.decomposition._nmf.NMF NMF_model - the NMF model
            - pd.DataFrame df_nmf_topics - a dataframe of topics with corresponding keywords
        """
        # Count Vectorizer
        vectorizer = CountVectorizer(analyzer='word', max_features=5000)
        x_counts = vectorizer.fit_transform(documents)
        
        # TF-IDF
        transformer = TfidfTransformer(smooth_idf=False)
        x_tfidf = transformer.fit_transform(x_counts)
        
        # Normalize
        xtfidf_norm = normalize(x_tfidf, norm='l1', axis=1)
        
        # NMF model
        NMF_model = NMF(n_components=num_topics, init='nndsvd')
        NMF_model.fit(xtfidf_norm)

        # create topic df
        feat_names = vectorizer.get_feature_names()
        df_nmf_topics = pd.DataFrame()
        
        for i in range(num_topics):
            words_ids = NMF_model.components_[i].argsort()[:-10 - 1:-1]
            words = [feat_names[key] for key in words_ids]
            df_nmf_topics['Topic_' + str(i + 1)] = words
     
        return NMF_model, df_nmf_topics
    
    def run_BERTopic_model(self):
        """
        Run BERTopic model
        
        :returns: 
            - bertopic._bertopic.BERTopic bert_model - the BERTopic model
            - list[int] topics - the topic number for each transcript
            - numpy.ndarray probs - the probability of topic for each transcript
            - pd.DataFrame df_bert_topics - a dataframe of topics with corresponding keywords
        """
        sentence_model = SentenceTransformer(self.model_path + 'all-MiniLM-L6-v2')
        bert_model = BERTopic(embedding_model=sentence_model)
        topics, probs = bert_model.fit_transform(self.docs)
        df_bert_topics = bert_model.get_topic_info()
        
        return bert_model, topics, probs, df_bert_topics
    
