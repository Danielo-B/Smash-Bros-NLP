#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 14:40:06 2019

@author: danielobennett
"""

import pandas as pd

#webscraping
import requests
from bs4 import BeautifulSoup
import pickle
import time
from pymongo import MongoClient
import json

user_agent = {'User-agent': '**********@****.com'} #personal email for authorization

#connect to mongoDB 
client = MongoClient()
db = client.Smash_tweets

'''
NEED TO START AND RUN MONGODB
'''

def GetPosts(topic_link, main_df): #add a any additional input
    '''
    gets all of the posts on all pages of one topic
    '''
    r = requests.get(topic_link, headers = user_agent)
    html_docn = r.text
    soup = BeautifulSoup(html_docn, features="lxml") 
    current_page = 1 #current page var
    try:
        topic_title = soup.find('title').text.split("- Super Smash Bros. Ultimate Message Board for Nintendo Switch")[0]
        posts = []  
        for message in soup.find_all(class_ ="msg_body newbeta"):
            posts.append(message.text.replace("\n", "")) #append 10 posts per page as list
        #make temp dataframe
        temp_df = pd.DataFrame({'Post': posts, 'Topic_Title': topic_title, 'Page_Num': current_page, 'Link': topic_link})
        main_df = pd.concat([main_df, temp_df], axis=0)
        ###Added appending to mongoDB
        temp_dict = temp_df.to_dict('records')
        db.Smash_posts.insert_many(temp_dict) # local mongoDB database
        
        
        #check if multiple pages > will process if such
        page_nums = 0
        #gets number of paginate tags, if none return an empty list
        var = list(soup.findAll(class_ = "paginate"))  
        if var: #checks if list is empty
            if 'Post New Message' in var[1].find('li').text.split('of ')[-1]: #stop incase paginated user and gets said string
                return main_df
            else:
                time.sleep(2) #to prevent IP blocking
                page_nums = int(var[1].find('li').text.split('of ')[-1])
                for number in range(1, page_nums): #non-inclusive
                    try:
                        page_url = topic_link + "?page=" + str(number)
                        #same as above should def make into a function
                        r_page = requests.get(page_url, headers = user_agent)
                        time.sleep(3) #to prevent IP blocking
                        html_doc_page = r_page.text
                        soup_page = BeautifulSoup(html_doc_page, features="lxml") 
                        current_page = number+1 #current page var
            #           topic_title = soup_page.find('title').text.split("- Super Smash Bros. Ultimate Message Board for Nintendo Switch")[0]
                        posts = []  
                        for message in soup_page.find_all(class_ ="msg_body newbeta"):
                            posts.append(message.text.replace("\n", "")) #append 10 posts per page as list            
                        temp_df2 = pd.DataFrame({'Post': posts, 'Topic_Title': topic_title, 'Page_Num': current_page, 'Link': page_url})
                        main_df = pd.concat([main_df, temp_df2], axis=0)
                        ###Added appending to mongoDB
                        temp_dict2 = temp_df2.to_dict('records')
                        db.Smash_posts.insert_many(temp_dict2)
                        print("processed page ", current_page)
                    except:
                        print("Pretty sure the function isnt working rn.. per page")                        
                return main_df
    except:
        print("Pretty sure the function isnt working rn..")

'''Real code starts here'''
#getting list of links
with open('topic_links.pkl', 'rb') as picklefile:
    list_of_links = pickle.load(picklefile)

'''
Current links to parse 
'''
#list_of_links = list_of_links[3844:] #use if need to process in subsets
#total length :) 4978


#goes link by link in list_of_links and gets posts for each page
final_df = pd.DataFrame()
for index, link in enumerate(list_of_links):
    time.sleep(2)
    print("Processing topic "+ str(index+7))
    GetPosts(link, final_df)
        