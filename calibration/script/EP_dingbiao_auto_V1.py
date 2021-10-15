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

sql_type = "old"
if sql_type == "new":
    engine = create_engine('mysql+mysqldb://root:root@192.168.2.58/pd?charset=utf8')
    fields_lights = {
        '275': {'a': 's_2', 'k': 'k_2', 'pw': 'pw_2', 'r': 'r2_2'},
        '365': {'a': 's_5', 'k': 'k_5', 'pw': 'pw_5', 'r': 'r2_5'},
        'Vis': {'a': 's_4', 'k': 'k_4', 'pw': 'pw_4', 'r': 'r2_4'}
    }
else:
    engine = create_engine('mysql+mysqldb://quantaread:CA*ySB^V7%wOff35@10.0.0.104/quantaapp?charset=utf8')
    fields_lights = {
        '275': {'a': 'a275', 'k': 'k_275', 'pw': 'pw_275', 'r': 'r_275'},
        '365': {'a': 'a365', 'k': 'k_365', 'pw': 'pw_365', 'r': 'r_365'},
        'Vis': {'a': 'aVis', 'k': 'k_Vis', 'pw': 'pw_Vis', 'r': 'r_Vis'}
    }


def query_all_devices(sectionName):
    """
    查询某批次的所有设备名称
    :param sectionName: 批次名称
    :return: 该批次做数据迁移的所有设备名称
    """
    if sql_type == 'old':
        sql = f"""
            SELECT DISTINCT deviceDesc
            FROM tb_qd_picketage_ep
            WHERE sectionName = '{sectionName}'
        """
    else:
        sql = f"""
            SELECT DISTINCT qda_uuid
            FROM calibration_dv_raw
            WHERE section_name = '{sectionName}' AND valid = '1'
        """
    all_devices = pd.read_sql(sql, con=engine).values.ravel().tolist()

    return all_devices


def get_all_raw_data(sectionName):
    all_devices = query_all_devices(sectionName)
    if len(all_devices) == 0:
        return None

    if sql_type == "old":
        sql = '''
            SELECT
                absorb.deviceId,
                absorb.deviceDesc,
                absorb.a275,
                absorb.a365,
                absorb.aVis,
                info.sampleType,
                info.sectionName,
                info.turbidity,
                info.collectionDate
            FROM
                tb_py_absorbance_EP AS absorb
            LEFT JOIN
                tb_qd_picketage_ep AS info
            ON
                absorb.deviceDesc = info.deviceDesc
                AND absorb.synCode = info.synCode
            WHERE
                info.sectionName = %(sectionName)s
                AND info.deviceDesc IN %(deviceDesc)s
                AND info.turbidity >= 0
                AND info.sampleType IN (1, 2)
            order by turbidity
            '''
        all_raw_data = pd.read_sql(sql, con=engine, params={'sectionName': sectionName, 'deviceDesc': all_devices})
        data_each_device = {device: all_raw_data[all_raw_data['deviceDesc'] == device] for device in all_devices}
    elif sql_type == "new":
        sql = f"""
            SELECT
                qda_uuid, s_2, s_5, s_4, sample_type, section_name, turbidity, collection_date
            FROM
                calibration_dv_raw
            WHERE
                section_name = '{sectionName}'
                AND qda_uuid IN {tuple(all_devices)}
                AND turbidity >= 0
                AND sample_type IN (1, 2)
                AND valid = '1'
            order by turbidity
            """
        all_raw_data = pd.read_sql(sql, con=engine)
        data_each_device = {device: all_raw_data[all_raw_data['qda_uuid'] == device] for device in all_devices}
    else:
        assert sql_type in ["new", "old"]

    return data_each_device


def get_transfer_data(deviceDesc):
    if sql_type == "new":
        sql = f"""
            SELECT
                pw_2, pw_5, pw_4, k_2, k_5, k_4
            FROM
                calibration_dv_full
            WHERE
                qda_uuid =  '{deviceDesc}'
            ORDER BY
                data_id DESC
            LIMIT 1
        """
    elif sql_type == "old":
        sql = f"""
            SELECT
                pw_275, pw_365, pw_Vis, k_275, k_365, k_Vis
            FROM
                tb_py_calibration_EP_dv
            WHERE
                deviceDesc = '{deviceDesc}'
            ORDER BY
                dataId DESC
            LIMIT 1
        """
    else:
        assert sql_type in ["new", "old"]

    transfer_data=pd.read_sql(sql, con=engine)
    return(transfer_data)


def is_satisfy(k, r):
    return not (k < 0.8 or k > 1.2 or r < 0.98)


class TransferDevice:
    '''
    transfer device with standard device
    '''
    def __init__(self, device_type, section_name, device_std):
        self.device_type = device_type
        self.section_name = section_name
        self.device_std = device_std
        self.devices = query_all_devices(section_name)
        self.raw_data = get_all_raw_data(section_name)
        self.standard_data_mean = None

    def get_device_abs_data(self, device_desc):
        # get device mean absorb data
        data = self.raw_data[device_desc]
        transfer_data = data.reset_index(drop=True)
        fields_abs = [fields_lights[k]['a'] for k in fields_lights]
        if sql_type == 'old':
            pure_water_absorb = list(
                transfer_data.loc[transfer_data['sampleType'] == 1, fields_abs].mean())
        else:
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
        # transfer devices, 1 mofify standard device, 2 transfer device
        if len(self.devices) == 0:
            return {}
        transfer_results = {}
        fitteds = {}
        self.modify_std_device()
        for device_desc in self.devices:
            pure_water_absorb, transfer_data_mean = self.get_device_abs_data(device_desc)

            device_info = {'qianyi': self.section_name,
                           'deviceDesc': device_desc,
                           'deviceStd': self.device_std,
                           'pw_275': round(pure_water_absorb[0], 6),
                           'pw_365': round(pure_water_absorb[1], 6),
                           'pw_Vis': round(pure_water_absorb[2], 6),
                           'satisfied': True}

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

                fit_dev[light] = [list(test_absorb.ravel()), list(standard_absorb.ravel()),
                                  list(test_absorb.ravel()), list(absorb_predict.ravel())]

                device_info['satisfied'] = is_satisfy(slope, r_square)
                device_info[fields_lights[light]['k']], device_info[fields_lights[light]['r']] = \
                    slope, r_square
                device_info['graph_' + light] = [list(test_absorb.ravel()), list(standard_absorb.ravel()),
                                                 list(test_absorb.ravel()), list(absorb_predict.ravel())]

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


def test():
    # 适用于EP系列数据迁移
    section_name = 'qianyi_20210824'
    standard_device = 'EP_00049'
    is_submit = False
    fitted_info. result = transfer(section_name, standard_device, is_submit)
    # plot(fitted_info, is_submit)


if __name__ == '__main__':
    # main()
    # test()
    section_name = 'qianyi_20210824'
    standard_device = 'EP_00049'
    standard_device = 'TD0045CB06-2011-0001'
    is_submit = False

    transfer_solver = TransferDevice("EP", section_name, standard_device)
    result = transfer_solver.transfer_device()

    if sql_type == "old":
        info, result1 = transfer1(section_name, standard_device, is_submit)
        keys = list(result.keys())
        for i in range(len(info)):
            ii = i // 3;
            ith = i % 3;
            d1 = np.array(info[i][:3]).ravel();
            d2 = np.array(result[keys[ii]]['dataset'][ith])[[0,1,3]].ravel();
            print(np.isclose(d1, d2))
        for i in range(len(result)):
            r1 = json.loads(result1)[i]
            for k in r1.keys():
                print(k)
                if "plot" in k or k == 'deviceId':
                    continue
                assert result[keys[i]][k] == r1[k]

