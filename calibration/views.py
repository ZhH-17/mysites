from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse, JsonResponse, Http404
from django.views.decorators.csrf import csrf_exempt

from rest_framework import status, generics
from rest_framework.decorators import api_view
from rest_framework.parsers import JSONParser
from rest_framework.response import Response
from rest_framework.request import clone_request
from sqlalchemy import create_engine
import pickle
import os
import json
import pandas as pd
import urllib
import pdb
from calibration.script.dingbiao_auto import TransferDevice

sql_url = 'mysql+mysqldb://root:root@192.168.2.58/pd?charset=utf8'


def convert_to_raw_data(data):
    pass


@api_view(['GET'])
def list_full_data(request):
    section_name = request.query_params.get('section_name', "")
    con = create_engine(sql_url)

    sql = f"select distinct qda_uuid from calibration_dv_raw where section_name='{section_name}'"
    df_qda_uuids = pd.read_sql(sql, con)
    qda_uuids = tuple(df_qda_uuids.values.reshape(-1).tolist())
    if len(qda_uuids) == 0:
        return Response({})

    sql = f"select * from calibration_dv_full where qda_uuid in {qda_uuids} order by data_id"
    data = pd.read_sql(sql, con)
    data = data[~data.duplicated(['qda_uuid'], keep='last')]
    data = df_qda_uuids.merge(data, left_on='qda_uuid', right_on='qda_uuid', how='left').fillna('')

    return Response({'raw_data': data.to_dict(orient='record')})


@api_view(['GET'])
def list_section_name(request):
    con = create_engine(sql_url)
    section_names = pd.read_sql(
        "select distinct section_name from calibration_dv_raw", con).dropna().values.reshape(-1).tolist()
    return Response({'section_names': section_names})


@api_view(['GET'])
def calculate_raw_data(request):
    if request.method == 'GET':
        section_name = request.query_params.get('sectionName', "")
        device_std = request.query_params.get('deviceStd', "")
        device_type = request.query_params.get('deviceType', "")
        con = create_engine(sql_url)
        transfer_solver = TransferDevice("EP", section_name, device_std)
        result = transfer_solver.transfer_device()
        return Response({'dataset': list(result.values())})


@api_view(['POST'])
def submit_full_data(request):
    if request.method == 'POST':
        pass


@api_view(['GET'])
def get_raw_data(request):
    qda_uuid = request.query_params.get('qdaUuid', "")
    device_id = request.query_params.get('deviceId', None)
    # should be '2020-06-03 16:32:24'
    date1 = request.query_params.get('beginTime', '2020-06-03 16:32:24')
    date2 = request.query_params.get('endTime', '2020-09-03 16:32:24')
    print('date: ', date1, date2)

    USERNAME = 'quantaeye'
    HOST = '60.205.227.86'
    DATABASE = 'quantaapp'
    PASSWORD = urllib.parse.quote_plus('Quanta@eye2018')
    engine = create_engine('mysql+mysqldb://{}:{}@{}/{}?charset=utf8'.format(USERNAME, PASSWORD, HOST, DATABASE))

    # try:
    #     device_id = pd.read_sql(
    #         f"select deviceId from tb_qda where qda_uuid='{qda_uuid}'", engine).values[0][0]
    # except:
    #     print("not find qda_uuid in tb_py_absorbance_EP")
    #     return Response({'message': "not find qda_uuid in tb_py_absorbance_EP"})

    print('deviceID: ', device_id)

    sql = f"""
            SELECT
                absorb.a275 s_2,
                absorb.a365 s_5,
                absorb.aVis s_4,
                absorb.synCode syn_code,
                qda.qda_uuid qda_uuid,
                qda.qda_hw_version hardware_version,
                qda.qda_soft_version firmware_version,
                ep2.collectionDate collection_date,
                ep2.sensorExposureTime275 t_2,
                ep2.sensorExposureTime365 t_5,
                ep2.sensorExposureTimeVis t_4,
                ep2.pd275 p_2,
                ep2.pd365 p_5,
                ep2.pdVis p_4,
                ep2.qdTemperature temperature,
                ep2.qdHumidity humidity,
                ep2.deviceId device_id,
                ep2.deviceDesc device_desc
            FROM
                tb_py_absorbance_EP AS absorb
            LEFT JOIN
                tb_qda AS qda
            ON
                absorb.deviceId = qda.deviceId
            LEFT JOIN
                tb_devicedata_ep2 as ep2
            on
                absorb.deviceId = ep2.deviceId
                AND absorb.synCode = ep2.synCode
            WHERE
                ep2.deviceId = '{device_id}'
                AND ep2.collectionDate >= '{date1}'
                AND ep2.collectionDate < '{date2}'
            order by collection_date
        """

    raw_data = pd.read_sql(sql, engine)
    return Response({'raw_data': raw_data.to_dict(orient='records')})


@api_view(['POST'])
def insert_raw_data(request):
    data = request.data
    selected = request.data['selected']
    form_data = request.data['formData']
    print(selected)
    print(form_data)
    [selected[i].update(form_data) for i in range(len(selected))]
    [selected[i].update({"valid": "1", "calibration_type": "水质定标"}) for i in range(len(selected))]

    print("insert raw data")
    print('data:', selected)
    df = pd.DataFrame()
    df = df.from_records(selected)
    print(df)
    con = create_engine(sql_url)
    try:
        df.to_sql('calibration_dv_raw', con, index=False, if_exists='append')
        return Response({'message': "insert success"})
    except:
        return Response({'message': "insert failed"})

    # sql = f"""
    #         SELECT
    #             absorb.a275 s_2,
    #             absorb.a365 s_5,
    #             absorb.aVis s_4,
    #             absorb.synCode syn_code,
    #             qda.qda_uuid qda_uuid,
    #             qda.qda_hw_version hardware_version,
    #             qda.qda_soft_version firmware_version,
    #             info.sampleType sample_type,
    #             info.sectionName section_name,
    #             info.turbidity turbidity,
    #             ep2.collectionDate collection_date,
    #             ep2.sensorExposureTime275 t_2,
    #             ep2.sensorExposureTime365 t_5,
    #             ep2.sensorExposureTimeVis t_4,
    #             ep2.pd275 p_2,
    #             ep2.pd365 p_5,
    #             ep2.pdVis p_4,
    #             ep2.qdTemperature temperature,
    #             ep2.qdHumidity humidity
    #         FROM
    #             tb_py_absorbance_EP AS absorb
    #         LEFT JOIN
    #             tb_qda AS qda
    #         ON
    #             absorb.deviceId = qda.deviceId
    #         LEFT JOIN
    #             tb_qd_picketage_ep AS info
    #         ON
    #             absorb.deviceId = info.deviceId
    #             AND absorb.synCode = info.synCode
    #         LEFT JOIN
    #             tb_devicedata_ep2 as ep2
    #         on
    #             absorb.deviceId = ep2.deviceId
    #             AND absorb.synCode = ep2.synCode
    #         WHERE
    #             info.deviceId = '{device_id}'
    #             AND ep2.collectionDate >= '{date1}'
    #             AND ep2.collectionDate < '{date2}'
    #             AND info.turbidity >= 0
    #             AND info.sampleType IN (1, 2)
    #         order by turbidity
    #     """
