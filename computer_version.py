#!/usr/bin/env python
# coding: utf-8
import cv2
import numpy as np
import matplotlib.pyplot as plt
import os
import json
import lxml
from bs4 import BeautifulSoup as bs
from pathlib import Path
import sys
import json
import csv
import pandas as pd
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azureml.core import Workspace, Dataset,Datastore
from azureml.data.datapath import DataPath
get_ipython().run_line_magic('matplotlib', 'inline')







class computer_version_rules:
    
    def __init__(self, input_path,local_output_path):
        
        self.input_path = input_path
        self.local_output_path = local_output_path



    def detect_stamp(self,date_name,batch_name, batch_path):
        detected_stamp_files = []
        #create ORB using SIFT
        orb = cv2.SIFT_create()
        #load training image
        train_image = cv2.imread('Capture.PNG')

        #convert the colour to grey
        train_image = cv2.cvtColor(train_image, cv2.COLOR_BGR2RGB)
        train_image_blur = cv2.GaussianBlur(train_image.copy(), (15, 15), 0)

        #display train image
        # fig = plt.figure(figsize=(12,12))
        # ax = fig.add_subplot(1,1,1)
        # ax.set_title('Stamp type to be detected')
        # ax.imshow(train_image_blur)

        kp1,des1 = orb.detectAndCompute(train_image_blur,None)

        #Iterating every image from each folder 
        for filename in os.listdir(batch_path):
            # print(filename)
            if filename.endswith('tif') or filename.endswith(".jpg") or filename.endswith(".pdf"):
                #load image
                # print(filename)
                img = cv2.imread(batch_path + filename)
                
                try:
                    #convert the colour to grey
                    img_to_match = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
                    img_to_match_blur = cv2.GaussianBlur(img_to_match.copy(), (21, 21), cv2.BORDER_DEFAULT)

                    #display test image
                    # fig = plt.figure(figsize=(12,12))
                    # ax = fig.add_subplot(1,1,1)
                    # ax.imshow(img_to_match)

                    #find the keypoints and descriptions of both train and test images
                    kp2,des2 = orb.detectAndCompute(img_to_match_blur,None)

                    if des2 is None: continue

                    bf = cv2.BFMatcher()
                    matches = bf.knnMatch(des1,des2,k=2)
                    good = []
                    
                    if len(matches[0]) !=2: continue
                    #set the distance between the matches
                    match_distance = 0.75
                    for m,n in matches:
                        if m.distance < match_distance*n.distance:
                            good.append([m])

                    # print(len(good))
                    
                    if len(good)>5:
                        # print(len(good))
                        # # we are considering the top 100 matches and hence good[:100]
                        # match_image = cv2.drawMatchesKnn(train_image,kp1,img_to_match,kp2,good[:100],None,flags=2)

                        # fig = plt.figure(figsize=(15,15))
                        # ax = fig.add_subplot(1,1,1)
                        # ax.imshow(match_image)
                        # plt.show()
                        file = date_name+'_'+batch_name+'_'+filename
                        # print(file)
                        detected_stamp_files.append(file)
                except:
                    continue
        return detected_stamp_files










