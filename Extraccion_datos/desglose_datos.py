# -*- coding: utf-8 -*-
"""
Created on Fri Dec 22 00:03:30 2023

@author: alber
"""

import os
import pandas as pd
from ast import literal_eval
import json
os.chdir('C:\\Users\\alber\\OneDrive\\Escritorio\\Universidad\\Master_Big_data\\Asignaturas\\TFM\\Scripts\\PRUB')



# CARGAMOS LOS DICCIONARIOS

with open("dicc_fcbarcelona4.json", "r") as file:
    dicc_fcbarcelona = json.load(file)


with open("dicc_mancity1.json", "r") as file:
    dicc_mancity = json.load(file)
    
    
with open("dicc_realmadrid1.json", "r") as file:
    dicc_realmadrid = json.load(file)    
    



#PASAMOS EL DICCIONARIO A DATAFRAME PARA POSTERIORMENTE PODER ANALIZARLO

df_fcbarcelona = pd.DataFrame(dicc_fcbarcelona) #Se crea un dataframe vacío para alojar los datos

df_fcbarcelona=df_fcbarcelona.T

ind_fcbarcelona=[]

for i in range(len(df_fcbarcelona)):
    ind_fcbarcelona.append(i)

df_fcbarcelona.index=ind_fcbarcelona






df_mancity= pd.DataFrame(dicc_mancity) #Se crea un dataframe vacío para alojar los datos

df_mancity=df_mancity.T

ind_mancity=[]

for i in range(len(df_mancity)):
    ind_mancity.append(i)

df_mancity.index=ind_mancity





df_realmadrid= pd.DataFrame(dicc_realmadrid) #Se crea un dataframe vacío para alojar los datos

df_realmadrid=df_realmadrid.T

ind_realmadrid=[]

for i in range(len(df_realmadrid)):
    ind_realmadrid.append(i)

df_realmadrid.index=ind_realmadrid




# Función para aplanar las columnas seleccionadas en dict
def flatten_columns(df, columns_to_flatten):
    df_result = df.copy()

    for col in columns_to_flatten:
        # Normalizar la columna indicada
        df_expanded = pd.json_normalize(df_result[col])

        # Renombrar las columnas creadas tras la normalización añadiendo el nombre de la columna original como prefijo
        df_expanded.columns = [f'{col}_{subcol}' for subcol in df_expanded.columns]

        # Concatenar las columnas obtenidas en el dataframe original
        df_result = pd.concat([df_result, df_expanded], axis=1)

        # Eliminar la columna original
        df_result = df_result.drop(col, axis=1)

    return df_result


# Función para detectar valores vacíos y sustituirlos
def columnas_nan_lista(df,nombre_columna):
    
    lista_limp= list(df[nombre_columna].fillna(""))
    lista_limp1=[]
    
    for i in range(len(lista_limp)):
        if lista_limp[i]=="":
            lista_limp1.append(list())
        else:
            lista_limp1.append(lista_limp[i])
            
    df[nombre_columna]=lista_limp1


# Función para detectar valores vacíos y sustituirlos
def columnas_nan_lista1(df,nombre_columna):
    
    lista_limp= list(df[nombre_columna].fillna(""))
    lista_limp1=[]
    
    for i in range(len(lista_limp)):
        if lista_limp[i]=="":
            lista_limp1.append(dict())
        else:
            lista_limp1.append(lista_limp[i])
            
    df[nombre_columna]=lista_limp1


# Función para detectar valores vacíos y sustituirlos
def columnas_nan_lista2(df,nombre_columna):
    
    lista_limp= list(df[nombre_columna].fillna(""))
    lista_limp1=[]
    
    for i in range(len(lista_limp)):
        if lista_limp[i]=="":
            lista_limp1.append(list(dict()))
        else:
            lista_limp1.append(lista_limp[i])
            
    df[nombre_columna]=lista_limp1
    
    
# Función para detectar valores vacíos
def columnas_nan_def(df,nombre_columna):
    
    lista_limp= list(df[nombre_columna].fillna(""))
    lista_limp1=[]
    
    for i in range(len(lista_limp)):
        if lista_limp[i]=="":
            lista_limp1.append("")
        else:
            lista_limp1.append(lista_limp[i])
            
    df[nombre_columna]=lista_limp1

#Función para detectar variables anidadas vacías
def comp_simb(df):
    n=list(df.columns)
    a=[]
    for i in range(len(df)):
        for j in n:
            if ("{" or "}") in str(df.iloc[i][j]):
                a.append(j)
    a=list(set(a))
    
    a=sorted(a)
    
    return a

#Función para detectar variables anidadas vacías de tipo lista o diccionario
def comp_colum(df):
    n=list(df.columns)
    a=[]
    for i in range(len(df)):
        for j in n:
            if type(df.iloc[i][j]) == dict or type(df.iloc[i][j]) == list:
                a.append(j)
    a=list(set(a))
    
    a=sorted(a)
    
    return a

#Función para detectar variables anidadas vacías de tipo diccionario
def comp_colum_dict(df):
    n=list(df.columns)
    a=[]
    for i in range(len(df)):
        for j in n:
            if type(df.iloc[i][j]) == dict:
                a.append(j)
    a=list(set(a))
    
    a=sorted(a)
    
    return a
            


#Desglose de variables para la cuenta @FCBARCELONA

comp_simb(df_fcbarcelona)

columns_to_flatten = ['anchors', 'author', 'challenges', 'contentLocation', 'contents', 'imagePost']


df_fcbarcelona_def= flatten_columns(df_fcbarcelona, columns_to_flatten)

columns_to_flatten1 = ['textExtra']

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten1)


textextra=[]

for i in range(0,17):
    textextra.append("textExtra_"+str(i))
    
df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, textextra)


contents=[]

for i in range(0,3):
    contents.append("contents_"+str(i))
    
df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, contents)

columns_to_flatten3 = ['contents_0_textExtra']

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten3)


contents1=[]

for i in range(0,16):
    contents1.append("contents_0_textExtra_"+str(i))

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, contents1)



columns_to_flatten4 = ['contents_1_textExtra']

lista_prueba= list(df_fcbarcelona_def['contents_1_textExtra'].fillna(""))

lista_prueba1= []

for i in range(len(lista_prueba)):
    if lista_prueba[i]=="":
        lista_prueba1.append(list())
    else:
        lista_prueba1.append(lista_prueba[i])

df_fcbarcelona_def['contents_1_textExtra']=lista_prueba1        
        
df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten4)
        

contents_1=[]

for i in range(0,17):
    contents_1.append("contents_1_textExtra_"+str(i))

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, contents_1)





columns_to_flatten5 = ['contents_2_textExtra']


columnas_nan_lista(df_fcbarcelona_def,'contents_2_textExtra')


df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten5)

contents_2=[]

for i in range(0,11):
    contents_2.append("contents_2_textExtra_"+str(i))

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, contents_2)


columns_to_flatten7 = ['anchors_0']

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten7)

columns_to_flatten8 = ['anchors_0_logExtra']

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten8)

columns_to_flatten9 = ['keywordTags']

columnas_nan_lista(df_fcbarcelona_def,'keywordTags')

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten9)


keywordsTags=[]

for i in range(0,10):
    keywordsTags.append("keywordTags_"+str(i))

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, keywordsTags)


challenges=[]

for i in range(0,16):
    challenges.append("challenges_"+str(i))

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, challenges)

columns_to_flatten12 = ['imagePost_images']

columnas_nan_lista(df_fcbarcelona_def,'imagePost_images')

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten12)


imagepost=[]

for i in range(0,10):
    imagepost.append("imagePost_images_"+str(i))

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, imagepost)


comp_simb(df_fcbarcelona_def)

columns_fcbarcelona=list(df_fcbarcelona_def.columns)

for i in columns_fcbarcelona:
    columnas_nan_def(df_fcbarcelona_def,str(i))
    
    
columns_to_flatten14 = comp_simb(df_fcbarcelona_def)
    
df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten14)


columns_to_flatten15 = comp_simb(df_fcbarcelona_def)  

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten15)

columns_to_flatten16 = comp_simb(df_fcbarcelona_def)  

df_fcbarcelona_def = flatten_columns(df_fcbarcelona_def, columns_to_flatten16)

comp_simb(df_fcbarcelona_def)  

df_fcbarcelona_def.to_excel("df_fcbarcelona1.xlsx")






#Desglose de variables para la cuenta @MANCITY

columns_to_flatten = ['author', 'challenges', 'contentLocation', 'contents', 'item_control']

df_mancity= flatten_columns(df_mancity, columns_to_flatten)

challenges=[]

for i in range(0,10):
    challenges.append("challenges_"+str(i))
    
for i in challenges:
    columnas_nan_lista1(df_mancity, str(i))
    
df_mancity= flatten_columns(df_mancity, challenges)


contents=[]

for i in range(0,5):
    contents.append("contents_"+str(i))

for i in contents:
    columnas_nan_lista1(df_mancity, str(i))

df_mancity= flatten_columns(df_mancity, contents)


contents_textExtra=[]

for i in range(0,5):
    contents_textExtra.append("contents_"+str(i)+"_textExtra")

for i in contents_textExtra:
    columnas_nan_lista2(df_mancity, str(i))

df_mancity= flatten_columns(df_mancity, contents_textExtra)


contents_0=[]

for i in range(0,6):
    contents_0.append("contents_0_textExtra_"+str(i))

for i in contents_0:
    columnas_nan_lista1(df_mancity, str(i))

df_mancity= flatten_columns(df_mancity, contents_0)


c=['contents_1_textExtra_0']

columnas_nan_lista1(df_mancity, 'contents_1_textExtra_0')

df_mancity= flatten_columns(df_mancity, c)


contents_2=[]

for i in range(0,10):
    contents_2.append("contents_2_textExtra_"+str(i))

for i in contents_2:
    columnas_nan_lista1(df_mancity, str(i))

df_mancity= flatten_columns(df_mancity, contents_2)


contents_3=[]

for i in range(0,7):
    contents_3.append("contents_3_textExtra_"+str(i))

for i in contents_3:
    columnas_nan_lista1(df_mancity, str(i))

df_mancity= flatten_columns(df_mancity, contents_3)


contents_4=[]

for i in range(0,10):
    contents_4.append("contents_4_textExtra_"+str(i))

for i in contents_4:
    columnas_nan_lista1(df_mancity, str(i))

df_mancity= flatten_columns(df_mancity, contents_4)


columns_to_flatten2 = ['contents_0_textExtra_6', 'contents_0_textExtra_7', 'contents_0_textExtra_8', 'contents_0_textExtra_9', 'contents_1_textExtra_1', 'contents_1_textExtra_2']

df_mancity= flatten_columns(df_mancity, columns_to_flatten2)


columns_to_flatten3 = ['contents_1_textExtra_3', 'music']


df_mancity= flatten_columns(df_mancity, columns_to_flatten3)


columns_to_flatten4 = ['stats','statsV2','textExtra','video']


df_mancity= flatten_columns(df_mancity, columns_to_flatten4)



columns_to_flatten5 = ['textExtra_0', 'textExtra_1', 'textExtra_2', 'textExtra_3', 'textExtra_4', 'textExtra_5', 'textExtra_6', 'textExtra_7', 'textExtra_8', 'textExtra_9', 'video_bitrateInfo', 'video_subtitleInfos']


df_mancity= flatten_columns(df_mancity, columns_to_flatten5)


video_bitrateInfo=[]

for i in range(0,6):
    video_bitrateInfo.append("video_bitrateInfo_"+str(i))

for i in video_bitrateInfo:
    columnas_nan_lista1(df_mancity, str(i))

df_mancity= flatten_columns(df_mancity, video_bitrateInfo)


video_subtitleInfos=[]

for i in range(0,16):
    video_subtitleInfos.append("video_subtitleInfos_"+str(i))

for i in video_subtitleInfos:
    columnas_nan_lista1(df_mancity, str(i))

df_mancity= flatten_columns(df_mancity, video_subtitleInfos)


columns_to_flatten6 = comp_simb(df_mancity)  


df_mancity = df_mancity.drop(columns_to_flatten6, axis=1)


columns_mancity=list(df_mancity.columns)

for i in columns_mancity:
    columnas_nan_def(df_mancity,str(i))



df_mancity.to_excel("df_mancity1.xlsx")







#Desglose de variables para la cuenta @REALMADRID

comp_simb(df_realmadrid)

comp_colum_dict(df_realmadrid)

columns_to_flatten=comp_colum_dict(df_realmadrid)


for i in columns_to_flatten:
    columnas_nan_lista(df_realmadrid, str(i))

df_realmadrid= flatten_columns(df_realmadrid, columns_to_flatten)

columns_to_flatten1=comp_simb(df_realmadrid)

for i in columns_to_flatten1:
    columnas_nan_lista(df_realmadrid, str(i))

df_realmadrid= flatten_columns(df_realmadrid, columns_to_flatten1)

columns_to_flatten2=comp_simb(df_realmadrid)

for i in columns_to_flatten2:
    columnas_nan_lista1(df_realmadrid, str(i))

df_realmadrid= flatten_columns(df_realmadrid, columns_to_flatten2)


columns_to_flatten3=comp_simb(df_realmadrid)

for i in columns_to_flatten3:
    columnas_nan_lista(df_realmadrid, str(i))

df_realmadrid= flatten_columns(df_realmadrid, columns_to_flatten3)


columns_to_flatten4=comp_simb(df_realmadrid)

for i in columns_to_flatten4:
    columnas_nan_lista1(df_realmadrid, str(i))

df_realmadrid= flatten_columns(df_realmadrid, columns_to_flatten4)


comp_simb(df_realmadrid)


columns_realmadrid=list(df_realmadrid.columns)

for i in columns_realmadrid:
    columnas_nan_def(df_realmadrid,str(i))

df_realmadrid.to_excel("df_realmadrid1.xlsx")





