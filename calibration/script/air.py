# -*- coding: utf-8 -*-
"""
Created on Tue May 11 14:40:49 2021

@author: lmguan
"""

import os
import sys
import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import warnings
import mysql.connector


warnings.filterwarnings("ignore")

USERNAME = 'quantaeye'
PASSWORD = 'Quanta@eye2018'
#HOST1 = '192.168.2.219'
HOST = '172.17.31.204'
DATABASE = 'quantaapp'
engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(USERNAME, PASSWORD, HOST, DATABASE))
#engine1 = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(USERNAME, PASSWORD, HOST1, DATABASE))

def insert(sql,params):
    conn = mysql.connector.connect(
        host=HOST,
        user='quantaeye',
        password='Quanta@eye2018',
        database='quantaapp',
        charset='utf8')
    cursor = conn.cursor()
    cursor.execute(sql, params)
    conn.commit()
def parser_DS_np2(byt):
    """parser plan EP (275 285 365 VIS, 有90个通道; NIR, 有120个通道"""
    d = np.ndarray((90, ), dtype=">i4", buffer=byt)
    return(d)
def array2str(h):
    n=len(h)
    s=str(h[0])
    for i in range(1,n):
        s+=','+str(h[i])
    return(s)
def search_qda_uuid(device_id):
    sql='''
        SELECT
            qda_uuid
        FROM
            tb_qda
        WHERE
            deviceId=%(deviceId)s
        '''
    ps = {'deviceId': device_id}
    data=pd.read_sql(sql, con=engine,params=ps)
    return(data)
def searchds(device_id,syn_code):
    sql='''
        SELECT
            deviceDesc,
            sensorDs275,
            sensorDs365,
            sensorDsVis,
            collectionDate
        FROM
            tb_devicedata_ep2
        WHERE
            deviceId=%(deviceId)s
        AND 
            synCode=%(synCode)s
        ORDER BY
            dataId DESC
        LIMIT 1
        '''
    ps = {'deviceId': device_id,'synCode':syn_code}
    data=pd.read_sql(sql, con=engine,params=ps)
    return(data)
def insertls(itype,device_id,device_Desc,qda_uuid,ds0,ds1,ds2,recordt):
    if itype=='w':
        tablen='lightshape'
    if itype=='a':
        tablen='lightshapeAir'
    if qda_uuid is None:
        sql='''
        INSERT INTO {}(deviceId,deviceDesc,ds0,ds1,ds2,createDate)
        VALUES (%s,%s,%s,%s,%s,%s)
        '''.format(tablen)
        params=(device_id,device_Desc,ds0,ds1,ds2,recordt)
    else:
        sql='''
        INSERT INTO {}(deviceId,deviceDesc,qda_uuid,ds0,ds1,ds2,createDate)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        '''.format(tablen)
        params=(device_id,device_Desc,qda_uuid,ds0,ds1,ds2,recordt)
    #print(params)
    insert(sql,params)
def main(data):
    # args = sys.argv[1:]
    # device_id = args[0]
    # syn_code = args[1]
    # itype = args[2]
    device_id = data.deviceId
    syn_code = data.synCode
    itype = data.itype
    data=searchds(device_id,syn_code)
    qda_data=search_qda_uuid(device_id)
    qda_uuid = None
    if not qda_data.empty:
        qda_uuid = qda_data['qda_uuid'][0]
    device_Desc=data['deviceDesc'][0]
    ds0=data['sensorDs275'][0]
    ds1=data['sensorDs365'][0]
    ds2=data['sensorDsVis'][0]
    recordt=data['collectionDate'][0]
    ds0=array2str(parser_DS_np2(ds0))
    ds1=array2str(parser_DS_np2(ds1))
    ds2=array2str(parser_DS_np2(ds2))
    insertls(itype,device_id,device_Desc,qda_uuid,ds0,ds1,ds2,recordt)

if __name__ == '__main__':
    main()
