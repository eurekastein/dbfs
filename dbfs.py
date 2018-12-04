import os, subprocess, zipfile
import shutil
import functools
import pandas as pd
from simpledbf import Dbf5
from sqlalchemy import create_engine

base_dir = '/media/kry/K/pytoys/dbfs/since2010'
psw = 'postgres'
user = 'postgres'
serv = 'localhost'
db = 'since'
ext = 'ext'

tablas = ['ageb_urb_caracteristicas_economicas.dbf', 'ageb_urb_caracteristicas_educativas.dbf',
       'ageb_urb_ageb_urb_discapacidad.dbf', 'ageb_urb_fecundidad.dbf', 'ageb_urb_hogares_censales.dbf',
       'ageb_urb_lengua_indigena.dbf', 'ageb_urb_migracion.dbf', 'ageb_urb_mortalidad.dbf', 
       'ageb_urb_religion.dbf','ageb_urb_servicios_de_salud.dbf', 'ageb_urb_situacion_conyugal.dbf',
       'ageb_urb_viviendas.dbf', 'estatal_caracteristicas_economicas.dbf', 'estatal_caracteristicas_educativas.dbf',
       'estatal_discapacidad.dbf', 'estatal_fecundidad.dbf', 'estatal_hogares_censales.dbf',
       'estatal_lengua_indigena.dbf', 'estatal_migracion.dbf', 'estatal_mortalidad.dbf', 
       'estatal_religion.dbf','estatal_servicios_de_salud.dbf', 'estatal_situacion_conyugal.dbf',
       'estatal_viviendas.dbf', 'loc_rur_caracteristicas_economicas.dbf', 'loc_rur_caracteristicas_educativas.dbf',
       'loc_rur_discapacidad.dbf', 'loc_rur_fecundidad.dbf', 'loc_rur_hogares_censales.dbf',
       'loc_rur_lengua_indigena.dbf', 'loc_rur_migracion.dbf', 'loc_rur_mortalidad.dbf', 
       'loc_rur_religion.dbf','loc_rur_servicios_de_salud.dbf', 'loc_rur_situacion_conyugal.dbf',
       'loc_rur_viviendas.dbf', 'loc_urb_caracteristicas_economicas.dbf', 'loc_urb_caracteristicas_educativas.dbf',
       'loc_urb_discapacidad.dbf', 'loc_urb_fecundidad.dbf', 'loc_urb_hogares_censales.dbf',
       'loc_urb_lengua_indigena.dbf', 'loc_urb_migracion.dbf', 'loc_urb_mortalidad.dbf', 
       'loc_urb_religion.dbf','loc_urb_servicios_de_salud.dbf', 'loc_urb_situacion_conyugal.dbf',
       'loc_urb_viviendas.dbf', 'manzanas_caracteristicas_economicas.dbf', 'manzanas_caracteristicas_educativas.dbf',
       'manzanas_discapacidad.dbf', 'manzanas_fecundidad.dbf', 'manzanas_hogares_censales.dbf',
       'manzanas_lengua_indigena.dbf', 'manzanas_migracion.dbf', 'manzanas_mortalidad.dbf', 
       'manzanas_religion.dbf','manzanas_servicios_de_salud.dbf', 'manzanas_situacion_conyugal.dbf',
       'manzanas_viviendas.dbf', 'municipal_caracteristicas_economicas.dbf', 'municipal_caracteristicas_educativas.dbf',
       'municipal_discapacidad.dbf', 'municipal_fecundidad.dbf', 'municipal_hogares_censales.dbf',
       'municipal_lengua_indigena.dbf', 'municipal_migracion.dbf', 'municipal_mortalidad.dbf', 
       'municipal_religion.dbf','municipal_servicios_de_salud.dbf', 'municipal_situacion_conyugal.dbf',
       'municipal_viviendas.dbf']


#creates the list of dbfs paths
dbf_path_list = []
for root, dirs, files in os.walk(base_dir):
    if root.endswith('tablas'):
        for dbf in files: 
            dbf_path = os.path.join(root, dbf)
            dbf_path_list.append(dbf_path)
        

for dbf in dbf_path_list: 
    dbf_list = []
    for tabla in tablas: 
        if dbf.endswith(tabla):
            dbf_path = Dbf5(dbf)                   
            dbf_list.append(dbf_path.to_dataframe())
            todos = pd.concat(dbf_list)
            engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(user,psw,serv,db))
            todos.to_sql(tabla, engine)
