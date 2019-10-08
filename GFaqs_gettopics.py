#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 14:28:13 2019

@author: danielobennett
"""

import numpy as np
import pandas as pd

#webscraping
import requests
from bs4 import BeautifulSoup
import pickle
import time


def GetPageLinks(URL, list_links):
    '''
    Retrives the links to topics on the Smash message boards.
    Takes input URL and the real_list
    Returns: Reallist with new links appended
    '''
    user_agent = {'User-agent': 'Mozilla/5.0'} #initialize user agent  
    try:    
        r = requests.get(URL, headers = user_agent)
        html_docn = r.text
        soup = BeautifulSoup(html_docn, features="lxml") 
        list_it = [] #list for all website links on 1 page
        for link in soup.findAll('a'):
            list_it.append(link.get('href'))
            list_it = list(dict.fromkeys(list_it))
            for link2 in list_it:
                if "boards/234547-super-smash-bros-ultimate/" in str(link2):
                    if "?filter=" not in str(link2): #get rid of special topics
                        if "?page=" not in str(link2): #get rid of additional pages
                            if "/answers/" not in str(link2): #get rid of answers threads
                                if "#" not in str(link2): #get rid of weird post linkings
                                    temp =  "https://gamefaqs.gamespot.com"  + link2
                                    list_links.append(temp)
        list_links = list(dict.fromkeys(list_links))
        return list_links
    except:
        list_links = list(dict.fromkeys(list_links))
        return list_links

'''
START OF ACTUAL CODE!!!!
'''
#Initialize empty list of links
real_links = []

#First run of the function (doesnt have page# in link)
URL = 'https://gamefaqs.gamespot.com/boards/234547-super-smash-bros-ultimate'
GetPageLinks(URL, real_links)


#Start of the loop to get multiple pages
for page in range(1,250):  #getting 250*20 topics
    time.sleep(5)
    print("Going to process board page:", page, "#ofLinks rn ", len(real_links))
    new_URL = "https://gamefaqs.gamespot.com/boards/234547-super-smash-bros-ultimate?page=" + str(page)
    GetPageLinks(new_URL, real_links)
    if page % 10 == 0:
        print("saving after ", str(page), "pages")
        with open('topic_links.pkl', 'wb') as picklefile:
            pickle.dump(real_links, picklefile)    

new_links = list(dict.fromkeys(real_links)) #get rid of duplicate links
with open('topic_links.pkl', 'wb') as picklefile:
    pickle.dump(new_links, picklefile)    

print("DONE!!! There are " + str(len(new_links))+ "links gathered from pages#:" + str(page))
