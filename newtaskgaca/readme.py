CALL INTENT PREDICTION

General Description

This repo is created by global customer advanced analytics team to develop a resusable code for Call Intent Prediction which can be utilized by different BUs. Call Intent Prediction is a data science product that can generate actionable, scalable insights from the call conversations between agent and the customer which can ultimately help the segment teams in reducing the call volumes. For this, we gather data from sources like conversations scripts from Amazon Connect, NPS surveys, Salesforce case data to generate standard list of keyword groups being talked about starting from high level like claims to the most specific level like accupunture_claims_enquiry.
There are three major modules of this project:
Call Data Preprocessor - This module combines input from various sources (Amazon call transcripts, ctr records, Salesforce) and extracts the necessary attributes along with the call transcripts which are further utilized by the other two modules.
Topic Miner - This module leverages NLP capabilities to identify the group of keywords that occur together in a topic. We utilized Latent Dirichlet Allocation (LDA), NMF, and BERTopic for our analysis.
Topic Search - The topics from Topic Miner are further assessed manually to form regex patterns. This module helps us to look for these patterns in the conversation transcripts, producing actionable insights.
More details can be found here: https://mfc.sharepoint.com/:f:/r/sites/O365_AdvancedCustomerAnalyticsCOE/Shared%20Documents/General/Predictive%20Servicing/Call%20Intent%20Prediction/Artifacts?csf=1&web=1&e=akKRxQ

Call Data Preprocessor

The call data pre-processor fetches the Amazon Connect production data, extracts relevant transcripts and contact records, combines it with salesforce data and then saves the output for further uses. The following steps are used to get the dtaa in the required form for further analysis:

Get relevant files’ paths and read all JSON files for a given day.
Generate a dataframe for transcripts information
Generate a dataframe for contact records information
Merge two dataframes using Contact_ID
Read the salesforce data for the business unit
Merge the salesforce data with the transcripts and contact records information using the Contact_ID to generate the output dataframe
Topic Miner

Topic mining is a method for unsupervised classification of documents, similar to clustering on numeric data. We leveraged the following NLP algorithms for topic modelling - Latent Dirichlet Allocation (LDA), Non Matrix Factorization (NMF), and BERTopic. These models can be used to find the set of keywords that occur together frequently as a topic. Since BERTopic provided most insightful results as compared to the other approaches, we utilized the topics from BERTopic to find the representative transcripts in each topic.

Since BERTopic provided most insightful results as compared to the other approaches, we utilized the topics from BERTopic to find the representative transcripts in each topic.

Topic Search

Given the regex patterns for different level of topics, pattern search processor would search if the conversation transcript texts contains the relevant regex patterns, and then generates the label columns for each topic. It uses the output of data pre-process as the input.The pattern search processor is designed to run on daily basis or looping through each day within a given date range. Patterns folder contains all regex expressions in text files, with folder structures representing the topic levels.

Steps to replicate the code for different BUs

Clone the repository
Run the data preprocessor main notebook (/Call_Data_Preprocessor/notebooks/Call_Data_Preprocessor_MainNotebook.py) and provide the following parameters:
if_today - whether we want to run the preprocessor for today (True/False)
start_date - the starting date for which we want to run the preprocessor
end_date - the end date for which we want to run the preprocessor
input_path: parent container where Amazon Connect data hosted
output_path: parent container where you will store transformed Amazon Connect Data
trans_keys: key=name you want for attribute, value=Attributes you want to select from transcript data
ctr_keys: key=name you want for attribute, value=Attributes you want to select from ctr data
mode: whether we want to run it in non-test mode or test mode for unit testing (test/non-test)
Run the topic miner main notebook (/Topic_Mining/TopicMining_MainNotebook.py) and provide the following parameters:
start_date: the start date used to filter data
end_date: the end date used to filter data
input_path: the path where the data preprocessor result is stored
model_path: the path of required NLP models
custom_stopwords_file_path: the path where custom stop word files are stored
manual_remove_stopwords: a list of custom stop words to be removed
manual_add_stopwords: a list of custom stop words to be added
language: the language by which we want to filter the data preprocessor data (currently it works for English data only)
business_unit: the business unit for which we want to do the analysis
Visualize the results using different Topic Mining algorithms - LDA, NMF and BERTopic. Infer the topic results and form different regex patterns based on it and organize them in a folder structure based on the topic hierarchy. Update the patterns hierarchy under the /Topic_Search/patterns folder.
Run the topic search main notebook (/Topic_Search/notebooks/TopicSearch_MainNotebook.py) and provide the following parameters:
if_today - whether we want to run the pattern search for today (True/False)
start_date - the starting date for which we want to run the pattern search
end_date - the end date for which we want to run the pattern search
input_path: parent container where we stored the data preprocessor results
output_path: parent container where we want to store the pattern search results
language: the language by which we want to filter the data preprocessor data (currently it works for English data only)
business_unit: the business unit for which we want to do the analysis
