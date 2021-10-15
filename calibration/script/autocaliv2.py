# -*- coding: utf-8 -*-
"""
Created on Thu Jul 29 19:04:28 2021

@author: lmguan
"""

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import warnings
import mysql.connector
import time
import matplotlib.dates as mdates
from datetime import datetime
from datetime import timedelta
USERNAME = 'quantaeye'
PASSWORD = 'Quanta@eye2018'
#HOST = '192.168.2.239'
HOST = '60.205.227.86'
DATABASE = 'quantaapp'
engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(USERNAME, PASSWORD, HOST, DATABASE))
#autocali('EP_00163','20210329100560',10.5,'tss','EP',method='o')
def autocali(deviceDesc,syncode,test,calitype,devtype,method='o'):
    if calitype=='cod' or calitype=='turb':
        autocalipast(deviceDesc,syncode,test,calitype,devtype)
    else:
        if method == 'k':
            autocaliother(deviceDesc,test,calitype)
        elif method=='o':
            if calitype=='codmn' or calitype=='toc':
                d=getprop(deviceDesc,syncode,'cod')
            elif calitype=='tss':
                d=getprop(deviceDesc,syncode,'degree')
            k=test/d
            autocaliother(deviceDesc,k,calitype)
    return()
def default(deviceDesc):
    sectionId,sectionName=getsectionId(deviceDesc)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if deviceDesc in ['S03D2103N10227','S03D2103N10204','S03D2103N10292']:
        paramsc={'section_name':sectionName,
             'section_id':sectionId,
             'k2_a275':'15.528','k_a275':'47.7042',
             'k2_a365':'0','k_a365':'0',
             'k2_aVis':'0','k_aVis':'0','b':'0.23',
             'record_time':now}
        paramsc=pd.DataFrame(paramsc,index=[0])
        paramst={'section_name':sectionName,
             'section_id':sectionId,
             'k2_a275':'0','k_a275':'0',
             'k2_a365':'21.145964','k_a365':'85.835165',
             'k2_aVis':'0','k_aVis':'0','b':'0.387857',
             'record_time':now}
        paramst=pd.DataFrame(paramst,index=[0])
    if deviceDesc in ['S03D2103N10291','S03D2103N10316','S03D2103N10198']:
        if deviceDesc=='S03D2103N10198':
            paramsc={'section_name':sectionName,
                    'section_id':sectionId,
                    'k2_a275':'-39.1629','k_a275':'271.396',
                    'k2_a365':'0','k_a365':'0',
                    'k2_aVis':'0','k_aVis':'0','b':'1.92567',
                    'record_time':now}
        if deviceDesc=='S03D2103N10291':
            paramsc={'section_name':sectionName,
                    'section_id':sectionId,
                    'k2_a275':'-18.6553','k_a275':'252.017',
                    'k2_a365':'0','k_a365':'0',
                    'k2_aVis':'0','k_aVis':'0','b':'1.89034',
                    'record_time':now}
        if deviceDesc=='S03D2103N10316':
            paramsc={'section_name':sectionName,
                    'section_id':sectionId,
                    'k2_a275':'-14.4644','k_a275':'258.047',
                    'k2_a365':'0','k_a365':'0',
                    'k2_aVis':'0','k_aVis':'0','b':'2.00572',
                    'record_time':now}

#        paramsc={'section_name':sectionName,
#             'section_id':sectionId,
#             'k2_a275':'-24.0246','k_a275':'265.222',
#             'k2_a365':'0','k_a365':'0',
#             'k2_aVis':'0','k_aVis':'0','b':'1.99104',
#             'record_time':now}
        paramsc=pd.DataFrame(paramsc,index=[0])
        paramst={'section_name':sectionName,
             'section_id':sectionId,
             'k2_a275':'0','k_a275':'0',
             'k2_a365':'57.4467','k_a365':'261.931',
             'k2_aVis':'0','k_aVis':'0','b':'1.47145',
             'record_time':now}
        paramst=pd.DataFrame(paramst,index=[0])
    paramks={'codmn':'0.3549','toc':'0.4','tss':'1.9094'}
    insertcali(sectionName,sectionId,paramsc,'cod',devtype='EP')
    insertcali(sectionName,sectionId,paramst,'turb',devtype='EP')
    for key in ['codmn','toc','tss']:
        autocaliother(deviceDesc,paramks[key],key)
def getprop(deviceDesc,syncode,p):
    sectionId,sectionName=getsectionId(deviceDesc)
    sql='''
        SELECT
            {}
        FROM
            tb_devicedata_ep2
        WHERE
            sectionName='{}'
        AND
            synCode='{}'
        '''.format(p,sectionName,syncode)
    data=pd.read_sql(sql, con=engine)
    return(data[p][0])
def autocalipast(deviceDesc,syncode,test,calitype,devtype):
    sectionId,sectionName=getsectionId(deviceDesc)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    paramsc={'section_name':sectionName,
             'section_id':sectionId,
             'k2_a275':0,'k_a275':0,
             'k2_a365':0,'k_a365':0,
             'k2_aVis':0,'k_aVis':0,'b':0,
             'record_time':now}
    a=getabsstd(sectionName,syncode,devtype)
    
    if len(a)==0:
        a=getabsstd(deviceDesc,syncode,devtype)
    if calitype=='cod':
        k=test/a['a275'][0]
        paramsc['k_a275']=str(round(k,3))
    if calitype=='turb':
        k=test/a['a365'][0]
        paramsc['k_a365']=str(round(k,3))
    
    for key in paramsc.keys():
        paramsc[key]=str(paramsc[key].values[0])
    insertcali(sectionName,sectionId,paramsc,calitype,devtype=devtype)
def autocaliother(deviceDesc,k,calitype):
    tablename='tb_py_calibration_'+calitype
    section_id,section_name=getsectionId(deviceDesc)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    k=str(round(float(k),4))
    params={'section_name':section_name,
            'section_id':section_id,
            'k':k,'b':0,'record_time':now}
    sql = '''
            INSERT INTO
                {}(section_name,section_id,k,b,record_time)
            VALUES({},{},{},{},{})
            '''.format(tablename,
            '\''+section_name+'\'',
            '\''+section_id+'\'',
            '\''+k+'\'',
            '0',
            '\''+now+'\'',)
    insert(sql,params)
def queryall(table_name,sectionId):
    sql_query = '''
        SELECT 
            *
        FROM 
            {}
        WHERE 
            section_id = %(section_id)s
        ORDER BY 
            data_id DESC
        LIMIT 1
        '''.format(table_name)
    ps = {'section_id': sectionId}
    data=pd.read_sql(sql_query, con=engine,params=ps)
    return(data)
def getabsstd(sectionName,syncode,devtype):
    tablename='tb_py_absorbance_'+devtype+'_std'
    sql='''
        SELECT
            a275,a365
        FROM
            {}
        WHERE
            sectionName='{}'
        AND
            synCode='{}'
    '''.format(tablename,sectionName,syncode)
    data=pd.read_sql(sql, con=engine)
    return(data)
def getcalip(sectionId,tableName,devtype):
    if devtype=='EPT':
        tableName='tb_py_calibration_EPT_section_'+tableName
    elif devtype=='EP':
        tableName='tb_py_calibration_EP_section_'+tableName
    data=queryall(tableName,sectionId)
    if len(data['k_a275'].values)==0:
        if devtype=='EPT':
            sectionId='ww_general'
        if devtype=='EP':
            sectionId='rw_general'
        data=queryall(tableName,sectionId)
    del data['data_id']
    return(data)
def insertcali(section_name,section_id,paramsc,calitype,devtype='EPT'):
    if devtype=='EP':
        sqlc = '''
            INSERT INTO
                tb_py_calibration_EP_section_{}(section_name,section_id,k2_a275,k_a275,k2_a365,k_a365,k2_aVis,k_aVis,b,record_time)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            '''.format(calitype)
    elif devtype=='EPT':
        sqlc = '''
            INSERT INTO
                tb_py_calibration_EPT_section_{}(section_name,section_id,k2_a275,k_a275,k2_a365,k_a365,k2_aVis,k_aVis,b,record_time)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            '''.format(calitype)
    values=(section_name,section_id,paramsc['k2_a275'].values[0],
            paramsc['k_a275'].values[0],paramsc['k2_a365'].values[0],paramsc['k_a365'].values[0],
            paramsc['k2_aVis'].values[0],paramsc['k_aVis'].values[0],paramsc['b'].values[0],paramsc['record_time'].values[0])
    insert(sqlc,values)
    return()
def getsectionId(deviceDesc):
    sql = '''
        SELECT
            sectionName,sectionId
        FROM
            tb_device
        WHERE
            deviceDesc = %(deviceDesc)s
        '''
    ps = {'deviceDesc': deviceDesc}
    data=pd.read_sql(sql, con=engine,params=ps)
    return(data['sectionId'].values[0],data['sectionName'].values[0])
def insert(sql,params):
    conn = mysql.connector.connect(
        host='60.205.227.86',
        #host='192.168.2.239',
        user='quantaeye',
        password='Quanta@eye2018',
        database='quantaapp',
        charset='utf8')
    cursor = conn.cursor()
    cursor.execute(sql, params)
    conn.commit()

