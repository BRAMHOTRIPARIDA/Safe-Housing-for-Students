# Safe_Housing_for_Students

import pandas as pd 
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import requests
import json
import sys
import nltk
import spacy
import locationtagger
from collections import Counter
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import string
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
# import nltk
# from nltk.tag import pos_tag
# from nltk.tag.stanford import StanfordNERTagger
# from nltk.chunk import tree2conlltags
# from nltk.tokenize import word_tokenize
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')
nltk.download('treebank')
nltk.download('averaged_perceptron_tagger')
import schedule
import time




def dps_code():

    # Web Scraping/Retrieving DPS Alerts Data

    links_df = pd.DataFrame()
    links_df['ALERT LINK']=''
    links_df

    links = []

    for i in range(1,50):
        #print("\n")
        alert_page_link = "https://dps.usc.edu/category/alerts/page/" + str(i) + "/"
        print("----------------", alert_page_link ,"----------------\n")
        
        URL = alert_page_link
        page = requests.get(URL)
        #print(page.text)
        soup = BeautifulSoup(page.content, "html.parser")
        #print(soup)
        
        results = soup.find(id="content")
        #print(results)

        job_elements = results.find_all("a", class_="read-more")
        #print(job_elements)    
        
        for link in job_elements:
            link_url = link["href"]
            #print(f"{link_url}\n")
            links.append(link_url)
        
    links
    links_df['ALERT LINK'] = links
    links_df    



    cnt=0
    ilist = []

    for i in range(len(links_df)):
        URL = links_df['ALERT LINK'][i]
        if("usc-alert" in URL or "resolution" in URL or "advisory" in URL or "notification" in URL):
            cnt=cnt+1
            print(URL)
            ilist.append(i)
    print(cnt)


    for j in range(len(ilist)):
        links_df = links_df.drop([ilist[j]])

    links_df = links_df.reset_index()
    links_df = links_df.drop(['index'],  axis = 1)
    links_df



    link_df = links_df.copy()
    link_df['INCIDENT TYPE']=''
    link_df['INCIDENT DESCRIPTION']=''
    link_df['DATE & TIME OF OCCURRENCE']=''
    link_df['LOCATION']=''
    #link_df['REPORT NUMBER']=''
    link_df['VEHICLE DESCRIPTION']=''
    link_df['SUSPECT DESCRIPTION']=''
    link_df




    #Populating fields

    for j in range(len(link_df)):
        URL = link_df['ALERT LINK'][j]
        #URL = 'https://dps.usc.edu/burglary-suspect-in-custody-3/'
        
        print(j,"---"+URL+"---")   
        page = requests.get(URL)
        #print(page.text)

        soup = BeautifulSoup(page.content, "html.parser")
        soup
        soup.prettify()

        results = soup.find(id="primary")
        results

        
        #************************************************************************************************
       
        
        title = results.find("h1", class_="entry-title")
        print("\nIncident Type : ",title.text)
        link_df['INCIDENT TYPE'].iloc[j] = title.text

        
        python_jobs1 = results.find_all("p")


        for i in range(len(python_jobs1)):
           
            if(":".lower() in python_jobs1[i].text.lower() and "Incident".lower() in python_jobs1[i].text.lower() and "Description".lower() in python_jobs1[i].text.lower()):
                if("Incident Description".lower() in python_jobs1[i].text.lower()):
                    incident_description = python_jobs1[i].text.rpartition('Incident Description')
                if("Description of Incident".lower() in python_jobs1[i].text.lower()):
                    incident_description = python_jobs1[i].text.rpartition('Description of Incident')
                key = incident_description[1]
                value = incident_description[2]
                #print(key, ":", value)
                value = value.replace(":", "", 1)
                value = value.strip()
                print(key, ":", value)
                link_df['INCIDENT DESCRIPTION'].iloc[j] = value
                continue

                
            if("Incident".lower() in python_jobs1[i].text.lower() and "Description".lower() in python_jobs1[i].text.lower()):
                if("Incident Description".lower() in python_jobs1[i].text.lower()):
                    incident_description = python_jobs1[i].text.rpartition('Incident Description')
                if("Description of Incident".lower() in python_jobs1[i].text.lower()):
                    incident_description = python_jobs1[i].text.rpartition('Description of Incident')
                key = incident_description[1]
                value = incident_description[2]
                #print(key, ":", value)
                value = value.replace(":", "", 1)
                value = value.strip()
                print(key, ":", value)
                link_df['INCIDENT DESCRIPTION'].iloc[j] = value
                continue
                

            if(":".lower() in python_jobs1[i].text.lower() and "DATE".lower() in python_jobs1[i].text.lower() and "TIME".lower() in python_jobs1[i].text.lower()):
                if("DATE & TIME OF OCCURRENCE".lower() in python_jobs1[i].text.lower()):
                    date_time = python_jobs1[i].text.rpartition('DATE & TIME OF OCCURRENCE')
                if("Time & date of incident".lower() in python_jobs1[i].text.lower()):
                    date_time = python_jobs1[i].text.rpartition('Time & date of incident')
                if("Date/Time".lower() in python_jobs1[i].text.lower()):
                    date_time = python_jobs1[i].text.rpartition('Date/Time')
                key = date_time[1]
                value = date_time[2]
                #print(key, ":", value)
                value = value.replace(":", "", 1)
                value = value.strip()
                print(key, ":", value)
                link_df['DATE & TIME OF OCCURRENCE'].iloc[j] = value
                continue
                

            if(":".lower() in python_jobs1[i].text.lower() and "LOCATION".lower() in python_jobs1[i].text.lower()):
                if("LOCATION" in python_jobs1[i].text):
                    location = python_jobs1[i].text.rpartition('LOCATION')
                if("Location" in python_jobs1[i].text):
                    location = python_jobs1[i].text.rpartition('Location')
                key = location[1]
                value = location[2]
                #print(key, ":", value)
                value = value.replace(":", "", 1)
                value = value.strip()
                print(key, ":", value)
                if("Location" in value):
                    value = value.replace("Location","")
                if("location" in value):
                    value = value.replace("location","")
                link_df['LOCATION'].iloc[j] = value
                continue

                
    #         if("REPORT NUMBER".lower() in python_jobs1[i].text.lower()):
    #             incident_description = python_jobs1[i].text.rpartition('REPORT NUMBER')
    #             key = incident_description[1]
    #             value = incident_description[2]
    #             #print(key, ":", value)
    #             value = value.replace(":", "", 1)
    #             value = value.strip()
    #             print(key, ":", value)
    #             link_df['REPORT NUMBER'].iloc[j] = value
    #             continue
        
        
            if(":".lower() in python_jobs1[i].text.lower() and "VEHICLE".lower() in python_jobs1[i].text.lower()):
                if("VEHICLE DESCRIPTION".lower() in python_jobs1[i].text.lower()):
                    vehicle = python_jobs1[i].text.rpartition('VEHICLE DESCRIPTION')
                if("SUSPECT VEHICLE".lower() in python_jobs1[i].text.lower()):
                    vehicle = python_jobs1[i].text.rpartition('SUSPECT VEHICLE')
                key = vehicle[1]
                value = vehicle[2]
                #print(key, ":", value)
                value = value.replace(":", "", 1)
                value = value.strip()
                print(key, ":", value)
                link_df['VEHICLE DESCRIPTION'].iloc[j] = value
                continue
             
            
            if(":".lower() in python_jobs1[i].text.lower() and "SUSPECT DESCRIPTION".lower() in python_jobs1[i].text.lower()):
                suspect = python_jobs1[i].text.rpartition('SUSPECT DESCRIPTION')
                key = suspect[1]
                value = suspect[2]
                #print(key, ":", value)
                value = value.replace(":", "", 1)
                value = value.strip()
                print(key, ":", value)
                link_df['SUSPECT DESCRIPTION'].iloc[j] = value
                continue
        
                
        if(link_df['INCIDENT DESCRIPTION'][j])=='':
            python_jobs1 = results.find("p")
            link_df['INCIDENT DESCRIPTION'][j] = python_jobs1.text
        print("\n")    
                
        



    #Populating empty fields

    for j in range(len(link_df)):
      
     for k in link_df.columns:
            
        if(link_df.iloc[j][k]==''):       
            #col = k

            URL = link_df['ALERT LINK'][j]
            #URL = 'https://dps.usc.edu/burglary-suspect-in-custody-3/'
            print(j,"---"+URL+"---")   
            page = requests.get(URL)
            #print(page.text)

            soup = BeautifulSoup(page.content, "html.parser")
            soup
            soup.prettify()

            results = soup.find(id="primary")
            results


            #************************************************************************************************


            title = results.find("h1", class_="entry-title")
            print("\nIncident Type : ",title.text)
            link_df['INCIDENT TYPE'].iloc[j] = title.text


            python_jobs1 = results.find_all("p")

            
            for i in range(len(python_jobs1)):
                if("Incident".lower() in python_jobs1[i].text.lower() and "Incident".lower() in k.lower()): 
                    if(":".lower() in python_jobs1[i].text.lower() and "Incident".lower() in python_jobs1[i].text.lower() and "Description".lower() in python_jobs1[i].text.lower()):
                        if("Incident Description".lower() in python_jobs1[i].text.lower()):
                            incident_description = python_jobs1[i].text.rpartition('Incident Description')
                        if("Description of Incident".lower() in python_jobs1[i].text.lower()):
                            incident_description = python_jobs1[i].text.rpartition('Description of Incident')
                        key = incident_description[1]
                        value = incident_description[2]
                        #print(key, ":", value)
                        value = value.replace(":", "", 1)
                        value = value.strip()
                        #print(key, ":", value)
                        link_df['INCIDENT DESCRIPTION'].iloc[j] = value
                        break


                if("Incident".lower() in python_jobs1[i].text.lower() and "Incident".lower() in k.lower()): 
                    if("Incident".lower() in python_jobs1[i].text.lower() and "Description".lower() in python_jobs1[i].text.lower()):
                        if("Incident Description".lower() in python_jobs1[i].text.lower()):
                            incident_description = python_jobs1[i].text.rpartition('Incident Description')
                        if("Description of Incident".lower() in python_jobs1[i].text.lower()):
                            incident_description = python_jobs1[i].text.rpartition('Description of Incident')
                        key = incident_description[1]
                        value = incident_description[2]
                        #print(key, ":", value)
                        value = value.replace(":", "", 1)
                        value = value.strip()
                        #print(key, ":", value)
                        link_df['INCIDENT DESCRIPTION'].iloc[j] = value
                        break


                if("Date".lower() in python_jobs1[i].text.lower() and "Date".lower() in k.lower()): 
                    if(":".lower() in python_jobs1[i].text.lower() and "DATE".lower() in python_jobs1[i].text.lower() and "TIME".lower() in python_jobs1[i].text.lower()):
                        if("DATE & TIME OF OCCURRENCE".lower() in python_jobs1[i].text.lower()):
                            date_time = python_jobs1[i].text.rpartition('DATE & TIME OF OCCURRENCE')
                        if("Time & date of incident".lower() in python_jobs1[i].text.lower()):
                            date_time = python_jobs1[i].text.rpartition('Time & date of incident')
                        if("Date/Time".lower() in python_jobs1[i].text.lower()):
                            date_time = python_jobs1[i].text.rpartition('Date/Time')
                        key = date_time[1]
                        value = date_time[2]
                        #print(key, ":", value)
                        value = value.replace(":", "", 1)
                        value = value.strip()
                        #print(key, ":", value)
                        link_df['DATE & TIME OF OCCURRENCE'].iloc[j] = value
                        break


                if("Location".lower() in python_jobs1[i].text.lower() and "Location".lower() in k.lower()): 
                    if(":".lower() in python_jobs1[i].text.lower() and "LOCATION".lower() in python_jobs1[i].text.lower()):
                        if("LOCATION" in python_jobs1[i].text):
                            location = python_jobs1[i].text.rpartition('LOCATION')
                        if("Location" in python_jobs1[i].text):
                            location = python_jobs1[i].text.rpartition('Location')
                        key = location[1]
                        value = location[2]
                        #print(key, ":", value)
                        value = value.replace(":", "", 1)
                        value = value.strip()
                        #print(key, ":", value)
                        if("Location" in value):
                            value = value.replace("Location","")
                        if("location" in value):
                            value = value.replace("location","")
                        link_df['LOCATION'].iloc[j] = value
                        break


                #         if("REPORT NUMBER".lower() in python_jobs1[i].text.lower()):
                #             incident_description = python_jobs1[i].text.rpartition('REPORT NUMBER')
                #             key = incident_description[1]
                #             value = incident_description[2]
                #             #print(key, ":", value)
                #             value = value.replace(":", "", 1)
                #             value = value.strip()
                #             print(key, ":", value)
                #             link_df['REPORT NUMBER'].iloc[j] = value
                #             continue


                if("Vehicle".lower() in python_jobs1[i].text.lower() and "Vehicle".lower() in k.lower()): 
                    if(":".lower() in python_jobs1[i].text.lower() and "VEHICLE".lower() in python_jobs1[i].text.lower()):
                        if("VEHICLE DESCRIPTION".lower() in python_jobs1[i].text.lower()):
                            vehicle = python_jobs1[i].text.rpartition('VEHICLE DESCRIPTION')
                        if("SUSPECT VEHICLE".lower() in python_jobs1[i].text.lower()):
                            vehicle = python_jobs1[i].text.rpartition('SUSPECT VEHICLE')
                        key = vehicle[1]
                        value = vehicle[2]
                        #print(key, ":", value)
                        value = value.replace(":", "", 1)
                        value = value.strip()
                        #print(key, ":", value)
                        link_df['VEHICLE DESCRIPTION'].iloc[j] = value
                        break


                if("SUSPECT DESCRIPTION".lower() in python_jobs1[i].text.lower() and "SUSPECT DESCRIPTION".lower() in k.lower()): 
                    if(":".lower() in python_jobs1[i].text.lower() and "SUSPECT DESCRIPTION".lower() in python_jobs1[i].text.lower()):
                        suspect = python_jobs1[i].text.rpartition('SUSPECT DESCRIPTION')
                        key = suspect[1]
                        value = suspect[2]
                        #print(key, ":", value)
                        value = value.replace(":", "", 1)
                        value = value.strip()
                        #print(key, ":", value)
                        link_df['SUSPECT DESCRIPTION'].iloc[j] = value
                        break


    link_df



    data_index = [i for i in range(1,len(link_df)+1)]
    data = link_df.copy()
    data['INDEX']=data_index
    data.set_index('INDEX', inplace=True)
    #data.to_excel("alerts.xlsx", encoding='utf8')
    data



    remove_locs=['Boulevard','Smart','Southeast','Similar','West','Jesse','Incident','Public Safety','DPS','Avenue',
    'Custody','Aggravated','Sexual','Texts','Date','IRN','South','Homeless','Original Incident','New','SUSPECT',
    'Black','Suspect','SUSPECTS','Hall','BLDG','Apartments','Custody Date','Alert','LA','Off Campus',
    'Field','O','Northwest','North','Alley','Ave', 'California', 'Los Angeles', 'Freeway','USC',
    'University','America','on St','Eastside', 'New', 'BLDG', 'Northeast', 'Disposition', 'DPS Camera']


    def remove_words(list1, remove_words):
        result = list(filter(lambda word: word not in remove_words, list1))
        return result



    chk_lst = ['Street','St','st','Boulevard','Blvd','Avenue','Field','Hall','Place','Ave','Parkway']

    loc_lst=[]

    rec_no=0
    for i in data['LOCATION']:
        rec_no=rec_no+1
        print("\n")
        print(rec_no)
        print(i)
        print("---------------------")
        
        loc=i
             
        #i = re.sub(r",", '', i)
        #i = re.sub(r";", '', i)
        
        i = i.translate(str.maketrans('', '', string.punctuation))
        i = i.strip()
        #print(i)

        
        str_lst=[]

        for sent in nltk.sent_tokenize(loc):
            for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent))):
                if hasattr(chunk, 'label'):
                    #print(chunk.label(), ' '.join(c[0] for c in chunk))
                    wrd = ' '.join(c[0] for c in chunk)
                    for k in range(len(i.split())):
                        if((i.split()[k] in wrd)):
                            if(k < len(i.split())-1):
                                wrd1 = i.split()[k+1]
                                if(('Street' not in wrd) and ('St' not in wrd) and ('st.' not in wrd) 
                                and ('Boulevard' not in wrd) and ('Blvd' not in wrd) and ('Avenue' not in wrd) 
                                and ('Ave' not in wrd) and ('Field' not in wrd)
                                and ('Hall' not in wrd) and ('Place' not in wrd)):
                                    if(wrd1 not in chk_lst):
                                        str_lst.append(wrd)  
                        
                
                
        if(i.count('Boulevard')>0):
            n=i.count('Boulevard')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Boulevard')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Boulevard')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])

                               
        if(i.count('Parkway')>0):
            n=i.count('Parkway')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Parkway')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Parkway')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])

                            
        if(i.count('Drive')>0):
            n=i.count('Drive')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Drive')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Drive')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])

                            
    #     if(i.count('Boulevard.')>0):
    #         n=i.count('Boulevard.')
    #         #print("n = ",n)
    #         for k in range(len(i.split())):
    #             if((i.split()[k]=='Boulevard.')):
    #                 #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
    #                     s = " ".join(str_lst)
    #                     sc = s.count('Boulevard.')
    #                     #print("str_lst = ", sc)
    #                     if(sc < n):
    #                         #print(i.split()[k-1]+" "+i.split()[k])
    #                         str_lst.append(i.split()[k-1]+" "+i.split()[k])
                            
                            
                            
        if(i.count('Blvd')>0):
            n=i.count('Blvd')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Blvd')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Blvd')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])

                            
    #     if(i.count('Blvd.')>0):
    #         n=i.count('Blvd.')
    #         #print("n = ",n)
    #         for k in range(len(i.split())):
    #             if((i.split()[k]=='Blvd.')):
    #                 #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
    #                     s = " ".join(str_lst)
    #                     sc = s.count('Blvd.')
    #                     #print("str_lst = ", sc)
    #                     if(sc < n):
    #                         #print(i.split()[k-1]+" "+i.split()[k])
    #                         str_lst.append(i.split()[k-1]+" "+i.split()[k])


                            
        if(i.count('Avenue')>0):
            n=i.count('Avenue')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Avenue')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Avenue')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])

                            
    #     if(i.count('Avenue.')>0):
    #         n=i.count('Avenue.')
    #         #print("n = ",n)
    #         for k in range(len(i.split())):
    #             if((i.split()[k]=='Avenue.')):
    #                 #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
    #                     s = " ".join(str_lst)
    #                     sc = s.count('Avenue.')
    #                     #print("str_lst = ", sc)
    #                     if(sc < n):
    #                         #print(i.split()[k-1]+" "+i.split()[k])
    #                         str_lst.append(i.split()[k-1]+" "+i.split()[k])
                            

        if(i.count('Ave')>0):
            n=i.count('Ave')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Ave')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Ave')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])

                            
        if(i.count('Field')>0):
            n=i.count('Field')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Field')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Field')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])


        if(i.count('Hall')>0):
            n=i.count('Hall')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Hall')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Hall')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])


        if(i.count('Place')>0):
            n=i.count('Place')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Place')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Place')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])


    #     if(i.count('Place.')>0):
    #         n=i.count('Place.')
    #         #print("n = ",n)
    #         for k in range(len(i.split())):
    #             if((i.split()[k]=='Place.')):
    #                 #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
    #                     s = " ".join(str_lst)
    #                     sc = s.count('Place.')
    #                     #print("str_lst = ", sc)
    #                     if(sc < n):
    #                         #print(i.split()[k-1]+" "+i.split()[k])
    #                         str_lst.append(i.split()[k-1]+" "+i.split()[k])

                            
        if(i.count('Street')>0):
            n=i.count('Street')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='Street')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('Street')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])

                            
    #     if(i.count('Street.')>0):
    #         n=i.count('Street.')
    #         #print("n = ",n)
    #         for k in range(len(i.split())):
    #             if((i.split()[k]=='Street.')):
    #                 #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
    #                     s = " ".join(str_lst)
    #                     sc = s.count('Street.')
    #                     #print("str_lst = ", sc)
    #                     if(sc < n):
    #                         #print(i.split()[k-1]+" "+i.split()[k])
    #                         str_lst.append(i.split()[k-1]+" "+i.split()[k])

        
        if(i.count('St')>0):
            n=i.count('St')
            #print("n = ",n)
            for k in range(len(i.split())):
                if((i.split()[k]=='St')):
                    #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
                        s = " ".join(str_lst)
                        sc = s.count('St')
                        #print("str_lst = ", sc)
                        if(sc < n):
                            #print(i.split()[k-1]+" "+i.split()[k])
                            str_lst.append(i.split()[k-1]+" "+i.split()[k])

                            
    #     if(i.count('St.')>0):
    #         n=i.count('St.')
    #         #print("n = ",n)
    #         for k in range(len(i.split())):
    #             if((i.split()[k]=='St.')):
    #                 #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
    #                     s = " ".join(str_lst)
    #                     sc = s.count('St.')
    #                     #print("str_lst = ", sc)
    #                     if(sc < n):
    #                         #print(i.split()[k-1]+" "+i.split()[k])
    #                         str_lst.append(i.split()[k-1]+" "+i.split()[k])

                            
    #     if(i.count('st.')>0):
    #         n=i.count('st.')
    #         #print("n = ",n)
    #         for k in range(len(i.split())):
    #             if((i.split()[k]=='st.')):
    #                 #if(("th" in i.split()[k-1][-2:]) or ("nd" in i.split()[k-1][-2:]) or ("st" in i.split()[k-1][-2:]) or ("rd" in i.split()[k-1][-2:])):
    #                     s = " ".join(str_lst)
    #                     sc = s.count('st.')
    #                     #print("str_lst = ", sc)
    #                     if(sc < n):
    #                         #print(i.split()[k-1]+" "+i.split()[k])
    #                         str_lst.append(i.split()[k-1]+" "+i.split()[k])
        
        
        for x in range(len(str_lst)):
            if('Street' in str_lst[x]):
                str_lst[x] = str_lst[x].replace('Street', 'St')

            if('Blvd' in str_lst[x]):
                str_lst[x] = str_lst[x].replace('Blvd', 'Boulevard')

            if('Avenue' in str_lst[x]):
                str_lst[x] = str_lst[x].replace('Avenue', 'Ave')

            if('28TH St' in str_lst[x]):
                str_lst[x] = str_lst[x].replace('28TH St', '28th St')

        str_lst = remove_words(str_lst, remove_locs)    
        
        for x in str_lst:
            for y in str_lst:
                if((x in y) and (x!=y)):
                    str_lst.remove(x)
                    
        str_lst = list(set(str_lst)) 

        for j in str_lst:
            print(j)
            loc_lst.append(j)


    loc_lst


    remove_locs=['Boulevard','Smart','Southeast','Similar','West','Jesse','Incident','Public Safety','DPS','Avenue',
    'Custody','Aggravated','Sexual','Texts','Date','IRN','South','Homeless','Original Incident','New','SUSPECT',
    'Black','Suspect','SUSPECTS','Hall','BLDG','Apartments','Custody Date','Alert','LA','Off Campus',
    'Field','O','Northwest','North','Alley','Ave', 'California', 'Los Angeles', 'Freeway','USC',
    'University','America','on St','Eastside', 'New', 'BLDG', 'Northeast', 'Disposition', 'DPS Camera']

    def remove_words(list1, remove_words):
        result = list(filter(lambda word: word not in remove_words, list1))
        return result

    print(len(loc_lst))
    loc_lst = remove_words(loc_lst, remove_locs)
    loc_lst
    print(len(loc_lst))



    my_dict = {i:loc_lst.count(i) for i in loc_lst}
    len(my_dict)
    my_dict

    loc_df = pd.DataFrame(list(my_dict.items()),columns = ['LOCATION','COUNT'])
    loc_df

    loc_df = loc_df.sort_values(by='COUNT', ascending=False)
    loc_df.head(50)


    loc_df = loc_df.reindex(['COUNT','LOCATION'], axis=1)
    loc_df['LOCATION'].unique()


    # MySQL

    #!pip install PyMySQL
    import pymysql
    pymysql.install_as_MySQLdb()
    from sqlalchemy import create_engine


    host = "****"
    port = ****
    dbname = "****"
    username = "****"
    password = "****"        


    db = pymysql.connect(host='****',
                                 user='****',
                                 password='****',                             
                                 db='****',
                                 port = ****) 
    print ("connect successful!!") 

    cursor = db.cursor()
    cursor

    cursor.execute("select version()")

    data1 = cursor.fetchone()
    data1



    # Queries

    loc_df_lst = [tuple(x) for x in loc_df.to_records(index=False)]
    loc_df_lst


    # Initial insert to dps_alert_locations_frequency
    # sql = "INSERT INTO dps_alert_locations_frequency(LOCATION,COUNT) values(%s, %s)"
    # cursor.executemany(sql, loc_df_lst)
    # db.commit()
    # print(cursor.rowcount, "was inserted.")



    sql = "ALTER TABLE dps_alert_locations_frequency DROP COLUMN ID"
    cursor.execute(sql)
    temp = cursor.fetchall()
    list(temp)


    sql = "ALTER TABLE housing_app_localerts DROP COLUMN ID"
    cursor.execute(sql)
    temp = cursor.fetchall()
    list(temp)


    sql = "UPDATE dps_alert_locations_frequency SET COUNT = %s WHERE LOCATION = %s"
    input_data = (loc_df['COUNT'], loc_df['LOCATION'])
    cursor.executemany(sql, loc_df_lst)  #input_data
    db.commit()
    print(cursor.rowcount, "was updated in dps_alert_locations_frequency")


    sql = "UPDATE housing_app_localerts SET COUNT = %s WHERE LOCATION = %s"
    input_data = (loc_df['COUNT'], loc_df['LOCATION'])
    cursor.executemany(sql, loc_df_lst)  #input_data
    db.commit()
    print(cursor.rowcount, "was updated in housing_app_localerts")


    sql = '''select * from dps_alert_locations_frequency;'''
    cursor.execute(sql)
    d1 = cursor.fetchall()
    list(d1)


    sql = '''select * from housing_app_localerts;'''
    cursor.execute(sql)
    d1 = cursor.fetchall()
    list(d1)



    d1_temp = []
    for i in loc_df_lst:
        j, k = i[1], i[0]
        jk = (j,k)
        #print(jk)
        if jk not in d1:
            #print(i)  
            d1_temp.append(jk)

    len(d1_temp)   
    d1_temp



    # Insert to dps_alert_locations_frequency

    sql = "INSERT INTO dps_alert_locations_frequency(LOCATION,COUNT) values(%s, %s)"
    cursor.executemany(sql, d1_temp)
    db.commit()
    print(cursor.rowcount, "was inserted into dps_alert_locations_frequency")


    # Insert to housing_app_localerts

    sql = "INSERT INTO housing_app_localerts(LOCATION,COUNT) values(%s, %s)"
    cursor.executemany(sql, d1_temp)
    db.commit()
    print(cursor.rowcount, "was inserted into housing_app_localerts")



    sql = "ALTER TABLE dps_alert_locations_frequency ADD COLUMN ID INT AUTO_INCREMENT UNIQUE FIRST"
    cursor.execute(sql)
    temp = cursor.fetchall()
    list(temp)


    sql = "ALTER TABLE housing_app_localerts ADD COLUMN ID INT AUTO_INCREMENT UNIQUE FIRST"
    cursor.execute(sql)
    temp = cursor.fetchall()
    list(temp)



    data_main = pd.DataFrame()
    data_main['INCIDENT TYPE']=data['INCIDENT TYPE']
    data_main['INCIDENT DESCRIPTION']=data['INCIDENT DESCRIPTION']
    data_main['DATE & TIME OF OCCURRENCE']=data['DATE & TIME OF OCCURRENCE']
    data_main['LOCATION']=data['LOCATION']
    data_main['VEHICLE DESCRIPTION']=data['VEHICLE DESCRIPTION']
    data_main['SUSPECT DESCRIPTION']=data['SUSPECT DESCRIPTION']
    data_main['ALERT LINK']=data['ALERT LINK']
    data_main


    data_lst = [tuple(x) for x in data_main.to_records(index=False)]
    data_lst



    # Initial insert to dps_alerts
    # sql = "INSERT INTO dps_alerts(ALERT_LINK,INCIDENT_TYPE,INCIDENT_DESCRIPTION,DATE_TIME_OCCURRENCE,LOCATION,VEHICLE_DESCRIPTION,SUSPECT_DESCRIPTION) values(%s, %s, %s, %s, %s, %s, %s)"
    # cursor.executemany(sql, data_lst)
    # db.commit()
    # print(cursor.rowcount, "was inserted.")


    sql = "UPDATE dps_alerts SET INCIDENT_TYPE= %s, INCIDENT_DESCRIPTION= %s, DATE_TIME_OCCURRENCE= %s, LOCATION= %s, VEHICLE_DESCRIPTION= %s, SUSPECT_DESCRIPTION = %s WHERE ALERT_LINK = %s"
    cursor.executemany(sql, data_lst)
    db.commit()
    print(cursor.rowcount, "was updated in dps_alerts")


    sql = '''select * from dps_alerts;'''
    cursor.execute(sql)
    d1 = cursor.fetchall()
    list(d1)


    d1_lst=[]
    for x in d1:
        y=x[0]
        d1_lst.append(y) 
    d1_lst


    d1_temp = []
    for i in data_lst:
        j0 = i[6]
        j = (j0)
        
        if(j not in d1_lst):
            d1_temp.append(j)
            break

    d1_temp   


    data_lst_2 = []
    for i in data_lst:
        if(i[6] in d1_temp):
            v = (i[6], i[0], i[1], i[2], i[3], i[4], i[5])
            data_lst_2.append(v)

    data_lst_2



    # Insert to dps_alerts
    sql = "INSERT INTO dps_alerts(ALERT_LINK,INCIDENT_TYPE,INCIDENT_DESCRIPTION,DATE_TIME_OCCURRENCE,LOCATION,VEHICLE_DESCRIPTION,SUSPECT_DESCRIPTION) values(%s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sql, data_lst_2)
    db.commit()
    print(cursor.rowcount, "was inserted into dps_alerts")



    # Insert to dps_alerts_temp
    # sql = "INSERT INTO dps_alerts_temp(ALERT_LINK,INCIDENT_TYPE,INCIDENT_DESCRIPTION,DATE_TIME_OCCURRENCE,LOCATION,VEHICLE_DESCRIPTION,SUSPECT_DESCRIPTION) values(%s, %s, %s, %s, %s, %s, %s)"
    # cursor.executemany(sql, data_lst)
    # db.commit()
    # print(cursor.rowcount, "was inserted.")


    # dps_alert_locations_frequency_ view update
    sql = "CREATE OR REPLACE VIEW dps_alert_locations_frequency_vw AS Select * from dps_alert_locations_frequency;"
    cursor.execute(sql)
    db.commit()


    # dps_alerts view update
    sql = "CREATE OR REPLACE VIEW dps_alerts_vw AS Select * from dps_alerts;"
    cursor.execute(sql)
    db.commit()

    cursor.close()



#schedule.every().second.do(dps_code)
schedule.every(12).hours.do(dps_code)


while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    time.sleep(1)


