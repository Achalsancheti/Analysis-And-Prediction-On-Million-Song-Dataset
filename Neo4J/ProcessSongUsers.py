
# coding: utf-8

# In[ ]:

import pandas as pd
from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "123456"))
sep_string = '"'
song_df = pd.read_csv("sanitizedFinalData.tsv", sep='\t')

array = song_df["SongID"].values
session = driver.session()

i = 0    
for a in array:
    i=i+1
    query = "CREATE(song:Song{songid:"+ sep_string + a + sep_string + "})"
    session.run(query)
    if (i%10000 == 0):
        print("Songs added: ", i)
print("Added all songs") 
session.close()






# In[ ]:

import pandas as pd

relation_df = pd.read_csv("aa.tsv", sep='\t')
uid = relation_df["UserID"].values
array = relation_df["Arr"].values        
i = 0

session = driver.session()
for a in array:
    ptr = a.strip("[]")
    ptr = ptr.strip("{}")
    ptr = ptr.split("},{")
    for b in ptr:
        ptr = b.split(",")
        query = ("MATCH (user:User{userid:"+ sep_string + uid[i] 
                 + sep_string +"}),(song:Song{songid:"+ sep_string + ptr[0]+ sep_string 
                 + "})" + "CREATE (user)-[:LISTENING{times:"+ sep_string +  ptr[1]+ sep_string
                 +"}]->(song)")
        session.run(query)
    i = i + 1
session.close()


# In[ ]:

sep_string = '"'
session = driver.session()
for a in array:    
    query = "CREATE (user:User{userid:"+ sep_string + uid[i] + sep_string + "})"
    i = i + 1
    session.run(query)
session.close()


# In[ ]:

import pandas as pd
import re
from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "123456"))
sep_string = '"'
song_df = pd.read_csv("sanitizedFinalData.tsv", sep='\t')
song_df = song_df.dropna()

songid_array = song_df["SongID"].values
art_name_array = song_df["Artist_name"].values
art_loc_array = song_df["Artist_location"].values

title_array = song_df["Title"].values
audience_array = song_df["User_count"].values
views_array = song_df["Num_Played"].values

artist_hotness_array = song_df["Artist_hotness"].values
song_hotness_array = song_df["Hotness"].values
duration_array = song_df["Duration"].values

session = driver.session()

i = 90300
j = 0
k = 0
for a in songid_array:
    session = driver.session()
    val1 = re.sub('[\"()\']\\//','',art_name_array[i]).strip('\t\n\r')
    val1 = val1.replace('\"','')
    val2 = re.sub('[\"()\'\\//]','',art_loc_array[i]).strip('\t\n\r')
    val2 = val2.replace('\"','')
    val3 = re.sub('[\"()\'\\//]','',title_array[i]).strip('\t\n\r')
    val3 = val3.replace('\"','')                        
    query = ("MATCH(song:Song{songid:"+ sep_string + a + sep_string 
    + "}) SET song.Artist_name = " + sep_string + val1 + sep_string  
    + ", song.Artist_location = " + sep_string + val2 + sep_string
    + ", song.Title = " + sep_string + val3 + sep_string
    + ", song.User_count = {}".format(audience_array[i])
    + ", song.Num_Played = {}".format(views_array[i])
    + ", song.Artist_hotness = {}".format(artist_hotness_array[i])
    + ", song.Hotness = {}".format(song_hotness_array[i])
    + ", song.Duration = {}".format(duration_array[i]))
    result = session.run(query)
    if result.peek:
        k = k + 1
    else:
        j = j + 1
        print(query)
    i=i+1
    session.close()
    if (i%1000 == 0):
        print("Songs added: ", i, j)
print("Added all songs") 
session.close()


# In[ ]:

session = driver.session()  
for a in array:
    query = "CREATE(song:Song{songid:"+ sep_string + a + sep_string + "})"
    session.run(query)
session.close()

