# -*- coding: utf-8 -*-
import os
import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')
import json
import numpy as np
import pandas as pd
import matplotlib
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import warnings
import pdb

warnings.filterwarnings("ignore")
USERNAME = 'quantaeye'
PASSWORD = 'Quanta@eye2018'
HOST = '172.17.31.204'
DATABASE = 'quantaapp'
# engine = create_engine('mysql+mysqldb://{}:{}@{}/{}?charset=utf8mb4'.format(USERNAME, PASSWORD, HOST, DATABASE))

# pwd_path, f = os.path.split(os.path.realpath(__file__))

sql_type = "new"
engine = create_engine('mysql+mysqldb://root:root@192.168.2.58/pd?charset=utf8')
fields_lights = {
    '2': {'a': 's_2', 'k': 'k_2', 'pw': 'pw_2', 'r': 'r2_2'},
    '5': {'a': 's_5', 'k': 'k_5', 'pw': 'pw_5', 'r': 'r2_5'},
    '4': {'a': 's_4', 'k': 'k_4', 'pw': 'pw_4', 'r': 'r2_4'}
}


# def query_all_devices(sectionName):
#     """
#     查询某批次的所有设备名称
#     :param sectionName: 批次名称
#     :return: 该批次做数据迁移的所有设备名称
#     """
#     sql = f"""
#         SELECT DISTINCT qda_uuid
#         FROM calibration_dv_raw
#         WHERE section_name = '{sectionName}' AND valid = '1'
#     """
#     all_devices = pd.read_sql(sql, con=engine).values.ravel().tolist()

#     return all_devices
def query_all_devices(section_name):
    """
    查询某批次的所有设备名称
    :param sectionName: 批次名称
    :return: 该批次做数据迁移的所有设备名称
    """
    sql = f"""
        SELECT DISTINCT qda_uuid, device_desc, device_id
        FROM calibration_dv_raw
        WHERE section_name = '{section_name}' AND valid = '1'
    """
    df = pd.read_sql(sql, con=engine)
    qda_uuids, device_descs, device_ids  = \
        df['qda_uuid'].tolist(), df['device_desc'].tolist(), df['device_id'].tolist()

    return qda_uuids, device_descs, device_ids


def get_all_raw_data(sectionName):
    qda_uuids, device_descs, device_ids = \
        query_all_devices(sectionName)
    if len(qda_uuids) == 0:
        return None, None, None, None

    sql = f"""
        SELECT
            qda_uuid, s_2, s_5, s_4, sample_type, section_name, turbidity, collection_date
        FROM
            calibration_dv_raw
        WHERE
            section_name = '{sectionName}'
            AND qda_uuid IN {tuple(qda_uuids)}
            AND turbidity >= 0
            AND sample_type IN (1, 2)
            AND valid = '1'
        order by turbidity
        """
    all_raw_data = pd.read_sql(sql, con=engine)
    data_each_device = {device: all_raw_data[all_raw_data['qda_uuid'] == device] for device in qda_uuids}

    return data_each_device, qda_uuids, device_descs, device_ids


def get_transfer_data(deviceDesc):
    sql = f"""
        SELECT
            pw_2, pw_5, pw_4, k_2, k_5, k_4
        FROM
            calibration_dv
        WHERE
            qda_uuid =  '{deviceDesc}'
        ORDER BY
            data_id DESC
        LIMIT 1
    """

    transfer_data=pd.read_sql(sql, con=engine)
    return(transfer_data)


def is_satisfy(k, r):
    if (k < 0.8 or k > 1.2 or r < 0.98):
        return '0'
    else:
        return '1'


class TransferDevice:
    '''
    transfer device with standard device
    '''
    def __init__(self, device_type, section_name, device_std):
        self.device_type = device_type
        self.section_name = section_name
        self.device_std = device_std
        # self.qda_uuids, self.device_descs, self.device_ids = query_all_devices(section_name)
        # self.devices = query_all_devices(section_name)
        self.raw_data, self.qda_uuids, self.device_descs, self.device_ids = \
            get_all_raw_data(section_name)
        self.standard_data_mean = None


    def get_device_abs_data(self, device_desc):
        # get device mean absorb data
        data = self.raw_data[device_desc]
        transfer_data = data.reset_index(drop=True)
        fields_abs = [fields_lights[k]['a'] for k in fields_lights]
        pure_water_absorb = list(
            transfer_data.loc[transfer_data['sample_type'] == 1, fields_abs].mean())
        transfer_data_mean = transfer_data.groupby(['turbidity'])[fields_abs].mean()
        return pure_water_absorb, transfer_data_mean

    def __get_std_device_abs_data(self):
        return self.get_device_abs_data(self.device_std)

    def modify_std_device(self):
        _, transfer_data_mean = self.__get_std_device_abs_data()
        ks = get_transfer_data(self.device_std)
        fields_abs = [fields_lights[k]['a'] for k in fields_lights]
        fields_pw = [fields_lights[k]['pw'] for k in fields_lights]
        fields_k = [fields_lights[k]['k'] for k in fields_lights]
        transfer_data_mean[fields_abs] = \
            (transfer_data_mean[fields_abs].values - ks[fields_pw].values) * ks[fields_k].values
        self.standard_data_mean = transfer_data_mean

    def transfer_device(self):
        # transfer qda, 1 mofify standard device, 2 transfer device
        if len(self.qda_uuids) == 0:
            return {'error': "not find devices in given date"}
        if self.device_std not in self.qda_uuids:
            return {'error': "check deviceStd value"}
        transfer_results = {}
        fitteds = {}
        self.modify_std_device()
        for i in range(len(self.qda_uuids)):
            qda_uuid, device_desc, device_id = \
                self.qda_uuids[i], self.device_descs[i], self.device_ids[i]
            pure_water_absorb, transfer_data_mean = self.get_device_abs_data(qda_uuid)

            device_info = {'qian_yi': self.section_name,
                           'qda_uuid': qda_uuid,
                           'device_desc': device_desc,
                           'device_id': device_id,
                           'device_std': self.device_std,
                           'pw_2': round(pure_water_absorb[0], 6),
                           'pw_5': round(pure_water_absorb[1], 6),
                           'pw_4': round(pure_water_absorb[2], 6),
                           'satisfied': '1'}

            merge_data = pd.merge(transfer_data_mean, self.standard_data_mean, on=['turbidity'])
            merge_data -= merge_data.iloc[0]  # 减掉纯水基线
            fit_dev = {}
            for light in fields_lights:
                test_absorb = \
                    np.array(merge_data[fields_lights[light]['a'] + '_x']).reshape(-1, 1)
                standard_absorb = \
                    np.array(merge_data[fields_lights[light]['a'] + '_y']).reshape(-1, 1)
                slope, r_square = fit_linear_regression(test_absorb, standard_absorb)
                absorb_predict = test_absorb * slope

                device_info['satisfied'] = is_satisfy(slope, r_square)
                device_info[fields_lights[light]['k']], device_info[fields_lights[light]['r']] = \
                    slope, r_square

                fit_dev[light] = [list(test_absorb.ravel()), list(standard_absorb.ravel()),
                                  list(test_absorb.ravel()), list(absorb_predict.ravel())]

                device_info['graph_' + light] = \
                    ",".join(map(str, list(test_absorb.ravel()))) + ":" + \
                    ",".join(map(str, list(standard_absorb.ravel()))) + ";" +\
                    ",".join(map(str, list(test_absorb.ravel()))) + ":" + \
                    ",".join(map(str, list(absorb_predict.ravel())))

            fitteds[device_desc] = fit_dev
            transfer_results[device_desc] = device_info
        return transfer_results


def fit_linear_regression(X_, y_):
    """
    计算新设备吸光度和样机吸光度之间的关系
    :param X_: 新设备在纯水和不同浓度梯度浊度液中的吸光度，扣掉该设备纯水吸光度之后的值
    :param y_: 样机设备在纯水和不同浓度梯度浊度液中的吸光度，扣掉样机纯水吸光度之后的值
    :return: 新设备吸光度和样机吸光度之间的换算关系(slope:斜率，r_square:相关系数)
    """
    reg = LinearRegression(fit_intercept=False)
    reg.fit(X_, y_)
    slope = round(reg.coef_[0, 0], 6)
    r_square = round(reg.score(X_, y_), 6)

    return slope, r_square


def main(data):
    # args = sys.argv[1:]
    # section_name = args[0]
    # standard_device = args[1]
    # is_submit = args[2]
    section_name = data.sectionName
    standard_device = data.deviceStd
    is_submit = data.submit

    transfer_solver = TransferDevice("EP", section_name, standard_device)
    result = transfer_solver.transfer_device()

    return result


if __name__ == '__main__':
    # main()
    # test()
    section_name = 'qianyi_20210824'
    standard_device = 'EP_00049'
    standard_device = 'TD0045CB06-2011-0001'
    is_submit = False

    transfer_solver = TransferDevice("EP", section_name, standard_device)
    result = transfer_solver.transfer_device()

