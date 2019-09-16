# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 13:49:08 2018

@author: NViani
Copy for reference for JayaC - This is to get documents from the CRIS SQL db
"""

import pandas as pd
import pyodbc
import pickle
import nltk

### read XLSX data ###

all_data = []

'''
### First data extraction ###
conn = pyodbc.connect("Driver={SQL Server Native Client 10.0};"
                      "Server=brhnsql094;"
                      "Database=SQLCRIS;"
                      "Trusted_Connection=yes;")

sql = ("SELECT [Summary] "+
      ",[Attachment_Text] "+
      ",[Attachment_Text_html] "+
      ",a.[ViewDate] "+
      ",a.[CN_Doc_ID] "+
      ",a.[BrcId] "+
      ",d.Brief_Summary "+
      ",d.Primary_Diag "+
      "FROM [SQLCRIS].[dbo].[Attachment] a, SQLCRIS.dbo.Discharge_Notification_Summary d "+
      "where "+
      "a.Summary like '%discharge summary%' "+
      "and a.BrcId = d.BrcId "+
      "and a.ViewDate = d.ViewDate")

df = pd.read_sql(sql, conn, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)

conn.close()

### Computed text length ###
def get_char_length(text):
    if text is None:
        return 0
    return len(text)

def get_tokens_num(text):
    if text is None:
        return 0
    tokens = nltk.word_tokenize(text)
    return len(tokens)

df['attach_tokens_num'] = df['Attachment_Text'].apply(get_tokens_num)
df['summary_tokens_num'] = df['Brief_Summary'].apply(get_tokens_num)

df.to_pickle("./discharge_summaries/attach_dis_summ.pkl")
'''

### Compute diagnosis stats and prepare new dataframe ###
df = pd.read_pickle("./discharge_summaries/attach_dis_summ.pkl")
df_pat = df.groupby('BrcId').size().reset_index(name='count')

print('Total rows:', len(df))
print('Rows with 0 tokens:', len(df[df['summary_tokens_num']==0]))
print('Rows with at least 5 tokens:', len(df[df['summary_tokens_num']>=5]))

def get_first_3_chars(diag):
    return diag[:3]

df['Primary_Diag_3_chars'] = df['Primary_Diag'].apply(get_first_3_chars)

df_diag = df.groupby('Primary_Diag').size().reset_index(name='count')
df_diag_sorted = df_diag.sort_values(['count'], ascending=[0])
print('Distinct diagnoses:', len(df_diag))

df_diag_3 = df.groupby('Primary_Diag_3_chars').size().reset_index(name='count')
df_diag_3_sorted = df_diag_3.sort_values(['count'], ascending=[0])
print('Distinct diagnoses (first 3 chars):', len(df_diag_3))

diag_common = ['F20','F29','F33','F25','F22','F23','F31','F32','F30','F28']
df_red = df[df['Primary_Diag_3_chars'].isin(diag_common)]
print('Documents with top 10 diagnoses (first 3 chars):', len(df_red))
print('Rows with 0 tokens:', len(df_red[df_red['summary_tokens_num']==0]))
print('Rows with at least 5 tokens:', len(df_red[df_red['summary_tokens_num']>=5]))

#df_red.to_pickle("./discharge_summaries_top_10.pkl")
all_diag = df['Primary_Diag'].unique().tolist()
all_diag.sort()

df_more_than_5 = df_red[df_red['attach_tokens_num']>5]
