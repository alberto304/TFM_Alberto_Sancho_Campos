# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 18:27:22 2024

@author: alber
"""

import os
import pandas as pd
from ast import literal_eval
import json
from ast import literal_eval

import packages
import numpy as np
import seaborn as sns

import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import matplotlib
from matplotlib.pyplot import figure
from datetime import datetime, timedelta

os.chdir('C:\\Users\\alber\\OneDrive\\Escritorio\\Universidad\\Master_Big_data\\Asignaturas\\TFM\\Scripts\\PRUB')



#Se define la función para sustituir variables vacías por ""
def columnas_nan(df,nombre_columna):
    
    lista_limp= list(df[nombre_columna].fillna(""))
    lista_limp1=[]
    
    for i in range(len(lista_limp)):
        if lista_limp[i]=="":
            lista_limp1.append("")
        else:
            lista_limp1.append(lista_limp[i])
            
    df[nombre_columna]=lista_limp1




df_fcbarcelona=pd.read_excel("df_fcbarcelona1.xlsx")



#Se comprueba cuantas variables son numéricas

df_numeric = df_fcbarcelona.select_dtypes(include=[np.number])
numeric_cols = df_numeric.columns.values
print(numeric_cols)
len(numeric_cols) 


#Las variables no numéricas

df_non_numeric = df_fcbarcelona.select_dtypes(exclude=[np.number])
non_numeric_cols = df_non_numeric.columns.values
print(non_numeric_cols)
len(non_numeric_cols) 


#Si visualizanlas primeras 30 variables con valores missing values señalados en amarillo

cols = df_fcbarcelona.columns[:30] # first 30 columns
colours = ['#000099', '#ffff00'] # specify the colours - yellow is missing. blue is not missing.
sns.heatmap(df_fcbarcelona[cols].isnull(), cmap=sns.color_palette(colours))



# Hacemos un listado para saber el porcentaje de missing values en nuestra base de datos

for col in df_fcbarcelona.columns:
    pct_missing = np.mean(df_fcbarcelona[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))



# Se eliminan las variables que tienen más de un 50% de missing values

variables=[]
porc=[]
for col in df_fcbarcelona.columns:
    pct_missing = np.mean(df_fcbarcelona[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))
    variables.append(col)
    porc.append(round(pct_missing*100))
    
    
variables_drop=[]

for i in range(0,len(porc)):
    if porc[i]>50:
        variables_drop.append(variables[i])

variables_drop=list(set(variables_drop))


df_fcbarcelona_def = df_fcbarcelona.drop(variables_drop, axis=1)
            

#Volvemos a comprobar si se han eliminado correctamente
for col in df_fcbarcelona_def.columns:
    pct_missing = np.mean(df_fcbarcelona_def[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))


#Se crea la función para detectar las variables compuestas por direcciones web
def comp_vac(df):
    n=list(df.columns)
    a=[]
    for i in range(len(df)):
        for j in n:
            if ("https") in str(df.iloc[i][j]):
                a.append(j)
    a=list(set(a))
    
    a=sorted(a)
    
    return a


#Se eliminan los registros con valores vacíos en formato "[]"
variables=[]
porc=[]
for col in df_fcbarcelona_def.columns:
    pct_missing = np.mean(df_fcbarcelona_def[col]=="[]")
    print('{} - {}%'.format(col, round(pct_missing*100)))
    variables.append(col)
    porc.append(round(pct_missing*100))
    
    
variables_drop=[]

for i in range(0,len(porc)):
    if porc[i]>50:
        variables_drop.append(variables[i])

variables_drop=list(set(variables_drop))


df_fcbarcelona_def = df_fcbarcelona_def.drop(variables_drop, axis=1)



# El dataframe tiene también imágenes, por lo que le faltan características en esta variable, así que se quedan con los vídeos

df_fcbarcelona_def = df_fcbarcelona_def[df_fcbarcelona_def['video_height']!=0]



#Se comprueban las columnas donde su contenido es repetitivo mayor al 80%, para así eliminarlas, ya que no nos aportarían nada

num_rows = len(df_fcbarcelona_def.index)
low_information_cols = [] #

for col in df_fcbarcelona_def.columns:
    cnts = df_fcbarcelona_def[col].value_counts(dropna=False)
    top_pct = (cnts/num_rows).iloc[0]

    if top_pct > 0.80:
        low_information_cols.append(col)
        print('{0}: {1:.5f}%'.format(col, top_pct*100))
        print(cnts)
        print()

# Se eliminan las columnas con datos repetitivos

df_fcbarcelona_def = df_fcbarcelona_def.drop(low_information_cols, axis=1)

#Y las que supuestamente incluyen direcciones web, ya que no nos sirven de nada

urls=comp_vac(df_fcbarcelona_def)


df_fcbarcelona_def = df_fcbarcelona_def.drop(urls, axis=1)

df_fcbarcelona_def = df_fcbarcelona_def.drop('contents_0_desc', axis=1)


#Se eliminan ciertas variables que comprobando no aportarían nada al estudio y son datos duplicados
columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if 'PlayAddr' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)

df_fcbarcelona_def = df_fcbarcelona_def.drop('challenges_3_desc', axis=1)



columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if 'contents_0_' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)


columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if 'challenges' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)


# Se combierten las fechas que están en formato POSIX a formato día/mes/año con la hora de subida

date_create=[]

for i in df_fcbarcelona_def.createTime:
    value=datetime.fromtimestamp(i)
    print(f"{value:%d/%m/%Y}")
    date_create.append(value)

#Ahora definimos la fecha en la que se realizó la extracción de datos a través de la API
fecha_ext=datetime.strptime('23/03/2024', '%d/%m/%Y')

#Y se realiza la diferencia entre fecha actual y cuando subió el vídeo a TikTok, esta diferencia estará definida en días
diferencia=[]

for i in date_create:
    print(round((fecha_ext-i)/ timedelta(days=1)))
    diferencia.append(round((fecha_ext-i)/ timedelta(days=1)))


df_fcbarcelona_def['dias_fecactual_fecpublic']=diferencia


#Se vuelve a comprobar y eliminar variables y datos que no aportan nada
columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if 'hashtagId' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)


columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if 'start' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)


columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if 'end' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)

df_fcbarcelona_def = df_fcbarcelona_def.drop('music_id', axis=1)



columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if '_type' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)

columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if '_subType' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)


columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if '_isCommerce' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)


columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if 'bitrateInfo' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)


#Se comprueba las variables que tienen missing values para así quitar los nan por ""

for i in df_fcbarcelona_def.columns:
    columnas_nan(df_fcbarcelona_def,str(i))


#Se crean variables dummies donde podremos señalar si esa musica se ha usado o no

#Se reemplaza también diversos datos unificando, ya que significan lo mismo
df_fcbarcelona_def=df_fcbarcelona_def.replace({"original sound": "sonido original"})

cambio_nom_music=[]

for i in df_fcbarcelona_def.music_title:
    cambio_nom_music.append("music_"+str(i))

df_fcbarcelona_def['music_title']=cambio_nom_music

music_title=list(set(df_fcbarcelona_def.music_title))

dummies_music = pd.get_dummies(df_fcbarcelona_def.music_title)

df_fcbarcelona_def=pd.concat([df_fcbarcelona_def, dummies_music], axis=1)


df_fcbarcelona_def = df_fcbarcelona_def.drop('music_title', axis=1)


df_fcbarcelona_def=df_fcbarcelona_def.replace({"barçaontiltok": "barçaontiktok", "iñaki": "iñakipeña", "torren":"torres", "vitor":"vitorroque"})




#Se convierte a minúscula todos los registros para evitar errores
df_fcbarcelona_def['textExtra_0_hashtagName']=df_fcbarcelona_def['textExtra_0_hashtagName'].str.lower()

hashtag=[]

for i in df_fcbarcelona_def.textExtra_0_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_fcbarcelona_def['textExtra_0_hashtagName']=hashtag



df_fcbarcelona_def['textExtra_1_hashtagName']=df_fcbarcelona_def['textExtra_1_hashtagName'].str.lower()

hashtag=[]

for i in df_fcbarcelona_def.textExtra_1_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_fcbarcelona_def['textExtra_1_hashtagName']=hashtag




df_fcbarcelona_def['textExtra_2_hashtagName']=df_fcbarcelona_def['textExtra_2_hashtagName'].str.lower()

hashtag=[]

for i in df_fcbarcelona_def.textExtra_2_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_fcbarcelona_def['textExtra_2_hashtagName']=hashtag



df_fcbarcelona_def['textExtra_3_hashtagName']=df_fcbarcelona_def['textExtra_3_hashtagName'].str.lower()

hashtag=[]

for i in df_fcbarcelona_def.textExtra_3_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_fcbarcelona_def['textExtra_3_hashtagName']=hashtag




df_fcbarcelona_def['textExtra_4_hashtagName']=df_fcbarcelona_def['textExtra_4_hashtagName'].str.lower()

hashtag=[]

for i in df_fcbarcelona_def.textExtra_4_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_fcbarcelona_def['textExtra_4_hashtagName']=hashtag



df_fcbarcelona_def['textExtra_5_hashtagName']=df_fcbarcelona_def['textExtra_5_hashtagName'].str.lower()

hashtag=[]

for i in df_fcbarcelona_def.textExtra_5_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_fcbarcelona_def['textExtra_5_hashtagName']=hashtag



df_fcbarcelona_def['textExtra_6_hashtagName']=df_fcbarcelona_def['textExtra_6_hashtagName'].str.lower()

hashtag=[]

for i in df_fcbarcelona_def.textExtra_6_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_fcbarcelona_def['textExtra_6_hashtagName']=hashtag



df_fcbarcelona_def['textExtra_7_hashtagName']=df_fcbarcelona_def['textExtra_7_hashtagName'].str.lower()

hashtag=[]

for i in df_fcbarcelona_def.textExtra_7_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_fcbarcelona_def['textExtra_7_hashtagName']=hashtag



#Se pasa a variables indicadoras las etiquetas o hashtags utilizados
dummies_hashtags = pd.get_dummies(df_fcbarcelona_def[["textExtra_0_hashtagName", "textExtra_1_hashtagName", "textExtra_2_hashtagName", "textExtra_3_hashtagName", "textExtra_4_hashtagName", "textExtra_5_hashtagName", "textExtra_6_hashtagName", "textExtra_7_hashtagName"]], prefix="", prefix_sep="").T.groupby(level=0).any().T             
dummies_hashtags = dummies_hashtags.drop("", axis=1)



df_fcbarcelona_def=pd.concat([df_fcbarcelona_def, dummies_hashtags], axis=1)



columnas=list(df_fcbarcelona_def.columns)
nombres=[]

for i in columnas:
    if 'textExtra_' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def = df_fcbarcelona_def.drop(nombres, axis=1)

#Y se eliminan las últimas variables que no sirven o duplican información
df_fcbarcelona_def = df_fcbarcelona_def.drop('Unnamed: 0', axis=1)


df_fcbarcelona_def = df_fcbarcelona_def.drop('createTime', axis=1)

df_fcbarcelona_def = df_fcbarcelona_def.drop('music_preciseDuration.preciseShootDuration', axis=1)

df_fcbarcelona_def = df_fcbarcelona_def.drop('music_preciseDuration.preciseAuditionDuration', axis=1)

df_fcbarcelona_def = df_fcbarcelona_def.drop('music_preciseDuration.preciseVideoDuration', axis=1)

df_fcbarcelona_def = df_fcbarcelona_def.drop('music_duration', axis=1)


#Se calcula la tasa de compromiso
df_fcbarcelona_def['tasa_compromiso']=(df_fcbarcelona_def['stats_diggCount']+df_fcbarcelona_def['stats_commentCount']+df_fcbarcelona_def['stats_shareCount'])/df_fcbarcelona_def['stats_playCount']





#REVISIÓN DE VALORES REPETIDOS EN DF_FCBARCELONA DESPUÉS DE LA LIMPIEZA

num_rows = len(df_fcbarcelona_def.index)
low_information_cols = [] #

for col in df_fcbarcelona_def.columns:
    cnts = df_fcbarcelona_def[col].value_counts(dropna=False)
    top_pct = (cnts/num_rows).iloc[0]

    if top_pct > 0.80:
        low_information_cols.append(col)
        print('{0}: {1:.5f}%'.format(col, top_pct*100))
        print(cnts)
        print()

# Se eliminan las columnas con datos repetitivos

df_fcbarcelona_def_PRUEBA = df_fcbarcelona_def.drop(low_information_cols, axis=1)



columnas=list(df_fcbarcelona_def_PRUEBA.columns)
nombres=[]

for i in columnas:
    if 'V2' in i:
        nombres.append(str(i))
        print(i)

df_fcbarcelona_def_PRUEBA = df_fcbarcelona_def_PRUEBA.drop(nombres, axis=1)



df_fcbarcelona_def_PRUEBA = df_fcbarcelona_def_PRUEBA.drop('desc', axis=1)


df_fcbarcelona_def_PRUEBA.to_excel("df_fcbarcelona_def_sindup1.xlsx")






###############################################################################

#Limpieza de datos para cuenta @REALMADRID

###############################################################################

df_realmadrid=pd.read_excel("df_realmadrid1.xlsx")



#Se comprueba cuantas variables son numéricas

df_numeric = df_realmadrid.select_dtypes(include=[np.number])
numeric_cols = df_numeric.columns.values
print(numeric_cols)
len(numeric_cols) 


#Las variables no numéricas

df_non_numeric = df_realmadrid.select_dtypes(exclude=[np.number])
non_numeric_cols = df_non_numeric.columns.values
print(non_numeric_cols)
len(non_numeric_cols) 


#Se visualiza las primeras 30 variables con valores missing values señalados en amarillo

cols = df_realmadrid.columns[:30] # first 30 columns
colours = ['#000099', '#ffff00'] # specify the colours - yellow is missing. blue is not missing.
sns.heatmap(df_realmadrid[cols].isnull(), cmap=sns.color_palette(colours))



# Hacemos un listado para saber el porcentaje de missing values en nuestra base de datos

for col in df_realmadrid.columns:
    pct_missing = np.mean(df_realmadrid[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))



# Se eliminan las variables que tienen más de un 50% de missing values

variables=[]
porc=[]
for col in df_realmadrid.columns:
    pct_missing = np.mean(df_realmadrid[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))
    variables.append(col)
    porc.append(round(pct_missing*100))
    
    
variables_drop=[]

for i in range(0,len(porc)):
    if porc[i]>50:
        variables_drop.append(variables[i])

variables_drop=list(set(variables_drop))


df_realmadrid_def = df_realmadrid.drop(variables_drop, axis=1)
            

#Volvemos a comprobar si se han eliminado correctamente
for col in df_realmadrid_def.columns:
    pct_missing = np.mean(df_realmadrid_def[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))


#Se crea la función para detectar las variables compuestas por direcciones web
def comp_vac(df):
    n=list(df.columns)
    a=[]
    for i in range(len(df)):
        for j in n:
            #if ("[]") in str(df.iloc[i][j]):
            if ("https") in str(df.iloc[i][j]):
                a.append(j)
    a=list(set(a))
    
    a=sorted(a)
    
    return a


#Se eliminan los registros con valores vacíos en formato "[]"
variables=[]
porc=[]
for col in df_realmadrid_def.columns:
    pct_missing = np.mean(df_realmadrid_def[col]=="[]")
    print('{} - {}%'.format(col, round(pct_missing*100)))
    variables.append(col)
    porc.append(round(pct_missing*100))
    
    
variables_drop=[]

for i in range(0,len(porc)):
    if porc[i]>50:
        variables_drop.append(variables[i])

variables_drop=list(set(variables_drop))


df_realmadrid_def = df_realmadrid_def.drop(variables_drop, axis=1)


# El dataframe tiene también imágenes, por lo que le faltan características en esta variable, así que se quedan con los vídeos

df_realmadrid_def = df_realmadrid_def[df_realmadrid_def['video_height']!=0]



#Se comprueba las columnas donde su contenido es repetitivo, para así eliminarlas, ya que no nos aportarían nada

num_rows = len(df_realmadrid_def.index)
low_information_cols = [] #

for col in df_realmadrid_def.columns:
    cnts = df_realmadrid_def[col].value_counts(dropna=False)
    top_pct = (cnts/num_rows).iloc[0]

    if top_pct > 0.80: 
        low_information_cols.append(col)
        print('{0}: {1:.5f}%'.format(col, top_pct*100))
        print(cnts)
        print()

# Se eliminan las columnas con datos repetitivos

df_realmadrid_def = df_realmadrid_def.drop(low_information_cols, axis=1)

#Y las que supuestamente incluyen direcciones web, ya que no nos sirven de nada

urls=comp_vac(df_realmadrid_def)


df_realmadrid_def = df_realmadrid_def.drop(urls, axis=1)


#Se eliminan ciertas variables que comprobando no aportarían nada al estudio y son datos duplicados
columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if 'PlayAddr' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)



columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if 'contents_0_' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)


columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if 'challenges' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)

# Se pasan las fechas que están en formato POSIX a formato día/mes/año con la hora de subida

date_create=[]

for i in df_realmadrid_def.createTime:
    value=datetime.fromtimestamp(i)
    print(f"{value:%d/%m/%Y}")
    date_create.append(value)

#Ahora definimos la fecha actual de este momento
fecha_ext=datetime.strptime('23/03/2024', '%d/%m/%Y')

#Y se realiza la diferencia entre fecha actual y cuando subió el vídeo a TikTok, esta diferencia estará definida en días
diferencia=[]

for i in date_create:
    print(round((fecha_ext-i)/ timedelta(days=1)))
    diferencia.append(round((fecha_ext-i)/ timedelta(days=1)))


df_realmadrid_def['dias_fecactual_fecpublic']=diferencia



#Se vuelve a comprobar y eliminar variables y datos que no aportan nada
columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if 'hashtagId' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)


columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if 'start' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)


columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if 'end' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)

df_realmadrid_def = df_realmadrid_def.drop('music_id', axis=1)



columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if '_type' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)



columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if '_subType' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)


columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if '_isCommerce' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)



columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if 'bitrateInfo' in i: #Revisar que significa los bitrateInfo
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)


#Se comprueban las variables que tienen missing values para así quitar los nan por ""

for i in df_realmadrid_def.columns:
    columnas_nan(df_realmadrid_def,str(i))


#Se reemplaza también diversos datos unificando, ya que significan lo mismo
df_realmadrid_def=df_realmadrid_def.replace({"sonido original - Real Madrid C.F.": "sonido original", "original sound": "sonido original", "original sound - Real Madrid C.F.": "sonido original", "suono originale":"sonido original"})

cambio_nom_music=[]

for i in df_realmadrid_def.music_title:
    cambio_nom_music.append("music_"+str(i))

df_realmadrid_def['music_title']=cambio_nom_music

music_title=list(set(df_realmadrid_def.music_title))

dummies_music = pd.get_dummies(df_realmadrid_def.music_title)

df_realmadrid_def=pd.concat([df_realmadrid_def, dummies_music], axis=1)


df_realmadrid_def = df_realmadrid_def.drop('music_title', axis=1)



columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if 'keyword' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)



df_realmadrid_def=df_realmadrid_def.replace({"madridistas?": "madridistas"})



#Se convierte a minúscula todos los registros para evitar errores
df_realmadrid_def['textExtra_0_hashtagName']=df_realmadrid_def['textExtra_0_hashtagName'].str.lower()

hashtag=[]

for i in df_realmadrid_def.textExtra_0_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_realmadrid_def['textExtra_0_hashtagName']=hashtag




df_realmadrid_def['textExtra_1_hashtagName']=df_realmadrid_def['textExtra_1_hashtagName'].str.lower()

hashtag1=[]

for i in df_realmadrid_def.textExtra_1_hashtagName:
    if i=="":
        hashtag1.append("")
    else:
        hashtag1.append("hashtagh_"+str(i))

df_realmadrid_def['textExtra_1_hashtagName']=hashtag1




df_realmadrid_def['textExtra_2_hashtagName']=df_realmadrid_def['textExtra_2_hashtagName'].str.lower()


hashtag2=[]

for i in df_realmadrid_def.textExtra_2_hashtagName:
    if i=="":
        hashtag2.append("")
    else:
        hashtag2.append("hashtagh_"+str(i))

df_realmadrid_def['textExtra_2_hashtagName']=hashtag2


#Se pasa a variables indicadoras las etiquetas o hashtags utilizados
dummies_hashtags = pd.get_dummies(df_realmadrid_def[["textExtra_0_hashtagName", "textExtra_1_hashtagName", "textExtra_2_hashtagName"]], prefix="", prefix_sep="").T.groupby(level=0).any().T             
dummies_hashtags = dummies_hashtags.drop("", axis=1)


df_realmadrid_def=pd.concat([df_realmadrid_def, dummies_hashtags], axis=1)



columnas=list(df_realmadrid_def.columns)
nombres=[]

for i in columnas:
    if 'textExtra_' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def = df_realmadrid_def.drop(nombres, axis=1)

#Y se eliminan las últimas variables que no sirven o duplican información
df_realmadrid_def = df_realmadrid_def.drop('Unnamed: 0', axis=1)

df_realmadrid_def = df_realmadrid_def.drop('createTime', axis=1)


df_realmadrid_def = df_realmadrid_def.drop('music_preciseDuration.preciseShootDuration', axis=1)

df_realmadrid_def = df_realmadrid_def.drop('music_preciseDuration.preciseAuditionDuration', axis=1)

df_realmadrid_def = df_realmadrid_def.drop('music_preciseDuration.preciseVideoDuration', axis=1)

df_realmadrid_def = df_realmadrid_def.drop('music_duration', axis=1)


#Se calcula la tasa de compromiso
df_realmadrid_def['tasa_compromiso']=(df_realmadrid_def['stats_diggCount']+df_realmadrid_def['stats_commentCount']+df_realmadrid_def['stats_shareCount'])/df_realmadrid_def['stats_playCount']



# MUSICA AUTOR
cambio_nom_music_author=[]

for i in df_realmadrid_def.music_authorName:
    cambio_nom_music_author.append("music_author_"+str(i))

df_realmadrid_def['music_authorName']=cambio_nom_music_author

music_authorName=list(set(df_realmadrid_def.music_authorName))

dummies_music_authorName = pd.get_dummies(df_realmadrid_def.music_authorName)

df_realmadrid_def=pd.concat([df_realmadrid_def, dummies_music_authorName], axis=1)


df_realmadrid_def = df_realmadrid_def.drop('music_authorName', axis=1)






# VOLVEMOS A REVISAR DUPLICIDADES UNA VEZ CREADOS LOS HASHTAGHS COMO DUMMIES

num_rows = len(df_realmadrid_def.index)
low_information_cols = [] #

for col in df_realmadrid_def.columns:
    cnts = df_realmadrid_def[col].value_counts(dropna=False)
    top_pct = (cnts/num_rows).iloc[0]

    if top_pct > 0.80: 
        low_information_cols.append(col)
        print('{0}: {1:.5f}%'.format(col, top_pct*100))
        print(cnts)
        print()

# Se eliminan las columnas con datos repetitivos

df_realmadrid_def_PRUEBA = df_realmadrid_def.drop(low_information_cols, axis=1)


df_realmadrid_def_PRUEBA = df_realmadrid_def_PRUEBA.drop('desc', axis=1)



columnas=list(df_realmadrid_def_PRUEBA.columns)
nombres=[]

for i in columnas:
    if 'V2' in i:
        nombres.append(str(i))
        print(i)

df_realmadrid_def_PRUEBA = df_realmadrid_def_PRUEBA.drop(nombres, axis=1)


#Se genera el excel final

df_realmadrid_def_PRUEBA.to_excel("df_realmadrid_def_sindup1.xlsx")




###############################################################################

# Limpieza para cuenta @MANCITY

###############################################################################

df_mancity=pd.read_excel("df_mancity1.xlsx")



#Se comprueba cuantas variables son numéricas

df_numeric = df_mancity.select_dtypes(include=[np.number])
numeric_cols = df_numeric.columns.values
print(numeric_cols)
len(numeric_cols) 


#Las variables no numéricas

df_non_numeric = df_mancity.select_dtypes(exclude=[np.number])
non_numeric_cols = df_non_numeric.columns.values
print(non_numeric_cols)
len(non_numeric_cols) 


#Se visualizan las primeras 30 variables con valores missing values señalados en amarillo

cols = df_mancity.columns[:30] # first 30 columns
colours = ['#000099', '#ffff00'] # specify the colours - yellow is missing. blue is not missing.
sns.heatmap(df_mancity[cols].isnull(), cmap=sns.color_palette(colours))



# Se hace un listado para saber el porcentaje de missing values en nuestra base de datos

for col in df_mancity.columns:
    pct_missing = np.mean(df_mancity[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))



# Se eliminan las variables que tienen más de un 50% de missing values

variables=[]
porc=[]
for col in df_mancity.columns:
    pct_missing = np.mean(df_mancity[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))
    variables.append(col)
    porc.append(round(pct_missing*100))
    
    
variables_drop=[]

for i in range(0,len(porc)):
    if porc[i]>50:
        variables_drop.append(variables[i])

variables_drop=list(set(variables_drop))


df_mancity_def = df_mancity.drop(variables_drop, axis=1)
         
   
#Se vuelve a comprobar si se han eliminado correctamente
for col in df_mancity_def.columns:
    pct_missing = np.mean(df_mancity_def[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))

#Se crea la función para detectar las variables compuestas por direcciones web
def comp_vac(df):
    n=list(df.columns)
    a=[]
    for i in range(len(df)):
        for j in n:
            #if ("[]") in str(df.iloc[i][j]):
            if ("https") in str(df.iloc[i][j]):
                a.append(j)
    a=list(set(a))
    
    a=sorted(a)
    
    return a


#Se eliminan los registros con valores vacíos en formato "[]"
variables=[]
porc=[]
for col in df_mancity_def.columns:
    pct_missing = np.mean(df_mancity_def[col]=="[]")
    print('{} - {}%'.format(col, round(pct_missing*100)))
    variables.append(col)
    porc.append(round(pct_missing*100))
    
    
variables_drop=[]

for i in range(0,len(porc)):
    if porc[i]>50:
        variables_drop.append(variables[i])

variables_drop=list(set(variables_drop))


df_mancity_def = df_mancity_def.drop(variables_drop, axis=1)

# El dataframe tiene también imágenes, por lo que le faltan características en esta variable, así que se quedan los vídeos


df_mancity_def = df_mancity_def[df_mancity_def['video_height']!=0]



#Se comprueban las columnas donde su contenido es repetitivo mayor al 80%, para así eliminarlas, ya que no nos aportarían nada

num_rows = len(df_mancity_def.index)
low_information_cols = [] #

for col in df_mancity_def.columns:
    cnts = df_mancity_def[col].value_counts(dropna=False)
    top_pct = (cnts/num_rows).iloc[0]

    if top_pct > 0.80: 
        low_information_cols.append(col)
        print('{0}: {1:.5f}%'.format(col, top_pct*100))
        print(cnts)
        print()

# Se eliminan las columnas con datos repetitivos

df_mancity_def = df_mancity_def.drop(low_information_cols, axis=1)

#Y las que supuestamente incluyen direcciones web, ya que no nos sirven de nada

urls=comp_vac(df_mancity_def)


df_mancity_def = df_mancity_def.drop(urls, axis=1)


#Se eliminan ciertas variables que comprobando no aportarían nada al estudio y son datos duplicados
columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'PlayAddr' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)



columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'contents_0_' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)


columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'challenges' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)

#Se convierten las fechas que están en formato POSIX a formato día/mes/año con la hora de subida

date_create=[]

for i in df_mancity_def.createTime:
    value=datetime.fromtimestamp(i)
    print(f"{value:%d/%m/%Y}")
    date_create.append(value)

#Ahora definimos la fecha actual de este momento
fecha_ext=datetime.strptime('23/03/2024', '%d/%m/%Y')

#Y se realiza la diferencia entre fecha actual y cuando subió el vídeo a TikTok, esta diferencia estará definida en días
diferencia=[]

for i in date_create:
    print(round((fecha_ext-i)/ timedelta(days=1)))
    diferencia.append(round((fecha_ext-i)/ timedelta(days=1)))


df_mancity_def['dias_fecactual_fecpublic']=diferencia



#Se vuelve a comprobar y eliminar variables y datos que no aportan nada
columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'hashtagId' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)


columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'start' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)


columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'end' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)

df_mancity_def = df_mancity_def.drop('music_id', axis=1)



columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if '_type' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)



columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if '_subType' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)


columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if '_isCommerce' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)



columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'bitrateInfo' in i: 
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)


#Se comprueba las variables que tienen missing values para así quitar los nan por ""

for i in df_mancity_def.columns:
    columnas_nan(df_mancity_def, str(i))



#Se crean variables dummies donde podremos señalar si esa musica se ha usado o no


columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'keyword' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)


#Se convierte a minúscula todos los registros para evitar errores
df_mancity_def['textExtra_0_hashtagName']=df_mancity_def['textExtra_0_hashtagName'].str.lower()

hashtag=[]

for i in df_mancity_def.textExtra_0_hashtagName:
    if i=="":
        hashtag.append("")
    else:
        hashtag.append("hashtagh_"+str(i))

df_mancity_def['textExtra_0_hashtagName']=hashtag




df_mancity_def['textExtra_1_hashtagName']=df_mancity_def['textExtra_1_hashtagName'].str.lower()

hashtag1=[]

for i in df_mancity_def.textExtra_1_hashtagName:
    if i=="":
        hashtag1.append("")
    else:
        hashtag1.append("hashtagh_"+str(i))

df_mancity_def['textExtra_1_hashtagName']=hashtag1




df_mancity_def['textExtra_2_hashtagName']=df_mancity_def['textExtra_2_hashtagName'].str.lower()


hashtag2=[]

for i in df_mancity_def.textExtra_2_hashtagName:
    if i=="":
        hashtag2.append("")
    else:
        hashtag2.append("hashtagh_"+str(i))

df_mancity_def['textExtra_2_hashtagName']=hashtag2



df_mancity_def['textExtra_3_hashtagName']=df_mancity_def['textExtra_3_hashtagName'].str.lower()

hashtag3=[]

for i in df_mancity_def.textExtra_3_hashtagName:
    if i=="":
        hashtag3.append("")
    else:
        hashtag3.append("hashtagh_"+str(i))

df_mancity_def['textExtra_3_hashtagName']=hashtag3





df_mancity_def['textExtra_4_hashtagName']=df_mancity_def['textExtra_4_hashtagName'].str.lower()

hashtag4=[]

for i in df_mancity_def.textExtra_4_hashtagName:
    if i=="":
        hashtag4.append("")
    else:
        hashtag4.append("hashtagh_"+str(i))

df_mancity_def['textExtra_4_hashtagName']=hashtag4




#Se pasa a variables indicadoras las etiquetas o hashtags utilizados

dummies_hashtags = pd.get_dummies(df_mancity_def[["textExtra_0_hashtagName", "textExtra_1_hashtagName", "textExtra_2_hashtagName", "textExtra_3_hashtagName", "textExtra_4_hashtagName"]], prefix="", prefix_sep="").T.groupby(level=0).any().T             
dummies_hashtags = dummies_hashtags.drop("", axis=1)


df_mancity_def=pd.concat([df_mancity_def, dummies_hashtags], axis=1)



columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'textExtra_' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)



columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'contents_' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def = df_mancity_def.drop(nombres, axis=1)


#Y se eliminan las últimas variables que no sirven o duplican información

df_mancity_def = df_mancity_def.drop('Unnamed: 0', axis=1)

df_mancity_def = df_mancity_def.drop('createTime', axis=1)


df_mancity_def = df_mancity_def.drop('music_preciseDuration.preciseShootDuration', axis=1)

df_mancity_def = df_mancity_def.drop('music_preciseDuration.preciseAuditionDuration', axis=1)

df_mancity_def = df_mancity_def.drop('music_preciseDuration.preciseVideoDuration', axis=1)

df_mancity_def = df_mancity_def.drop('music_duration', axis=1)


#Se calcula la tasa de compromiso
df_mancity_def['tasa_compromiso']=(df_mancity_def['stats_diggCount']+df_mancity_def['stats_commentCount']+df_mancity_def['stats_shareCount'])/df_mancity_def['stats_playCount']




#Se vuelve a comprobar duplicidades en variables

num_rows = len(df_mancity_def.index)
low_information_cols = [] #

for col in df_mancity_def.columns:
    cnts = df_mancity_def[col].value_counts(dropna=False)
    top_pct = (cnts/num_rows).iloc[0]

    if top_pct > 0.80: 
        low_information_cols.append(col)
        print('{0}: {1:.5f}%'.format(col, top_pct*100))
        print(cnts)
        print()

# Se eliminan las columnas con datos repetitivos

df_mancity_def_PRUEBA = df_mancity_def.drop(low_information_cols, axis=1)



columnas=list(df_mancity_def.columns)
nombres=[]

for i in columnas:
    if 'V2' in i:
        nombres.append(str(i))
        print(i)

df_mancity_def_PRUEBA = df_mancity_def_PRUEBA.drop(nombres, axis=1)


#Se genera el excel

df_mancity_def_PRUEBA.to_excel("df_mancity_def_sindup1.xlsx")


