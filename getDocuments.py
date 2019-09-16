# -*- coding: utf-8 -*-
"""
Created on Mon Mar 19 14:21:30 2018

@author: NViani
Reference copy for JayaC
This is to get documents as separate files in batches to upload into eHOST eventually for annotations
"""

import pandas as pd
import pickle
import random
import nltk

def get_tokens_num(t):
    text = str(t)
    if text is None:
        return 0
    tokens = nltk.word_tokenize(text) 
    return len(tokens)

def produceDocuments(df):
    
    f_batch = open('batches.txt', "w")
    
    import os
    import shutil
    outputDir="extraction-updated-attributes/"
    #shutil.copyfile("T:/Natalia Viani/event_work/event_annotation/annotationadmin.xml", os.path.join(outputDir,"annotationadmin.xml"))
    
    i = 0 #num_patients per batch
    j = 1 #num_batches
    k = 1 #num_doc
    prev_brcid = "first"
    
    df=df.sort_values(by=['BrcId','date'])
    
    for t in df[['BrcId', 'CN_Doc_ID', 'TextContent','date']].itertuples():
          
        brcidcol = str(t[1])
        docidcol = str(t[2])
        textcol = str(t[3])
        date = str(t[4])
        
        if(brcidcol!=prev_brcid):
            i +=1
            k = 1
        
        if i/11==1:
            j += 1
            i = 1
            
        batch = 'batch_'+str(j)
        patient = 'pat_'+brcidcol
        
        batch_directory = os.path.join(outputDir, batch)
        if not os.path.isdir(batch_directory):
            os.mkdir(batch_directory)
            
        pat_directory = os.path.join(batch_directory, patient)
        if not os.path.isdir(pat_directory):
            os.mkdir(pat_directory)
            
        corpusdirectory = os.path.join(pat_directory,  "corpus")
        if not os.path.isdir(corpusdirectory):
            os.mkdir(corpusdirectory)
            
        saveddirectory = os.path.join(pat_directory, "saved")
        if not os.path.isdir(saveddirectory):
            os.mkdir(saveddirectory)
            
        configdirectory = os.path.join(pat_directory, "config")
        if not os.path.isdir(configdirectory):
            shutil.copytree("T:/Natalia Viani/annotation_prescription/Jaya/depression/config", configdirectory)
        
        configdirectory = os.path.join(pat_directory, "config")
        shutil.copy("S:/All apps/Development/Natalia/annotation_projects/config_tasks/prescription_correction/projectschema.xml", configdirectory)        
        
        date_list = date.split(" ")
        fname = str(k)+"-"+docidcol+"-"+date_list[0]+".txt"
        print(j, i, fname)
        f_batch.write(str(j)+"\t"+brcidcol+"\t"+str(i)+"\t"+str(fname)+"\n")
        f = open(os.path.join(corpusdirectory, fname), "w")
        f.write(textcol)
        f.close() 
        prev_brcid = brcidcol
        k +=1
        
    f_batch.close()
    return None


##to read saved data ###
input_file = './../../../../TRD_text_examples/trd_nlp_spec_20190320_current.xlsx'

#read patient-firstDrug sheet
df1 = pd.read_excel(input_file, sheetname=1)
df1_pat = df1.groupby(['brcid']).size().reset_index(name="counts")
print('number of patients',len(df1_pat)) #100

#read text sheet
df2 = pd.read_excel(input_file, sheetname=2)
df2_pat = df2.groupby(['BrcId']).size().reset_index(name="counts")
print('number of patients with documents',len(df2_pat)) #95

df2['text_tokens_num'] = df2['TextContent'].apply(get_tokens_num)

pat1 = df1['brcid'].tolist()
pat2 = df2['BrcId'].tolist()
diff = set(pat1)-set(pat2)

#{10086605, 10098669, 10152893, 10183675, 10211901} no documents
print('number of docs',len(df2))
#2466 docs
print(df2_pat.describe())

#take patients with #docs in 75% percentile
df2_pat_red = df2_pat[df2_pat['counts']<=27.5]
selected_patients = df2_pat_red['BrcId'].unique()
print('number of patients with less than 27.5 documents',len(df2_pat_red)) #71

df_selected = df2[df2["BrcId"].isin(selected_patients)] #778 docs
df_selected_pat = df_selected.groupby(['BrcId']).size().reset_index(name="counts")
print('final number of documents',len(df_selected))

#df_selected.to_excel('docs_for_annotation.xlsx')

produceDocuments(df_selected)

#df_selected_pat_tokens = df_selected.groupby(['BrcId'])['text_tokens_num'].agg('sum')

#df_selected_docs = df_selected.groupby(['BrcId'])['CN_Doc_ID'].size().reset_index(name='count')