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

warnings.filterwarnings("ignore")

USERNAME = 'quantaeye'
PASSWORD = 'Quanta@eye2018'
HOST = '172.17.31.204'
#HOST = '60.205.227.86'
DATABASE = 'quantaapp'
engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(USERNAME, PASSWORD, HOST, DATABASE))

path = '/home/quantaeye/python/fitted_lines'

if not os.path.exists(path):
    os.makedirs(path)
    

def query_all_devices(sectionName):
    """
    查询某批次的所有设备名称
    :param sectionName: 批次名称
    :return: 该批次做数据迁移的所有设备名称
    """
    sql = '''
        SELECT
            DISTINCT deviceDesc
        FROM
            tb_qd_picketage_ep
        WHERE
            sectionName = %(sectionName)s
        '''
    all_devices = pd.read_sql(sql, con=engine, params={'sectionName': sectionName})['deviceDesc'].tolist()
    return all_devices


def get_all_raw_data(sectionName):
    all_devices = query_all_devices(sectionName)
    sql = '''
        SELECT
            absorb.deviceId,
            absorb.deviceDesc,
            absorb.qda_uuid,
            absorb.a275,
            absorb.a365,
            absorb.aVis,
            info.sampleType,
            info.sectionName,
            info.turbidity,
            info.collectionDate
        FROM
            tb_py_absorbance_EPT AS absorb
        INNER JOIN 
            tb_qd_picketage_ep AS info 
        ON
            absorb.deviceDesc = info.deviceDesc
            AND absorb.synCode = info.synCode
        WHERE
            info.sectionName = %(sectionName)s
            AND info.deviceDesc IN %(deviceDesc)s
            AND info.turbidity >= 0
            AND info.sampleType IN (1, 2)
        '''
    all_raw_data = pd.read_sql(sql, con=engine, params={'sectionName': sectionName, 'deviceDesc': all_devices})
    data_each_device = {device: all_raw_data[all_raw_data['deviceDesc'] == device] for device in all_devices}
    #print(data_each_device)
    return data_each_device
def get_transfer_data(qda_uuid,deviceDesc):
    sql = '''
        SELECT
            pw_275, pw_365, pw_Vis, k_275, k_365, k_Vis
        FROM
            tb_py_calibration_EPT_dv
        WHERE
            qda_uuid = '{}'
        OR
            deviceDesc = '{}'
        ORDER BY 
            dataId DESC
        LIMIT 1
        '''.format(qda_uuid,deviceDesc)
    transfer_data=pd.read_sql(sql, con=engine)
    return(transfer_data)

class TransferEPlus:
    def __init__(self, qianyi, deviceDesc, deviceStd):
        """
        :param qianyi: 批次名称
        :param deviceDesc: 待迁移设备名称
        :param deviceStd: 样机名称
        """
        self.qianyi = qianyi
        self.deviceDesc = deviceDesc
        self.deviceStd = deviceStd
        self.deviceId = None
        self.qda_uuid=None
        self.pure_water_absorb = None  # 纯水吸光度
        self.transfer_data_mean = None  # group by 取平均后的纯水吸光度和各NTU浊度液的吸光度
    def get_device_raw_data(self):
        """
        从数据库中提取定标原始数据，计算新设备纯水吸光度，和不同浓度梯度浊度液下的吸光度，存储到TransferEPlus class中
        """
        raw_data = raw_data_[self.deviceDesc]
        if raw_data.empty:
            return False
        transfer_data = raw_data.reset_index(drop=True)
        # print(transfer_data)
        self.deviceId = transfer_data.loc[0, 'deviceId']
        self.qda_uuid = transfer_data.loc[0, 'qda_uuid']
        self.pure_water_absorb = list(
            transfer_data.loc[transfer_data['sampleType'] == 1, ['a275', 'a365', 'aVis']].mean())
        self.transfer_data_mean = transfer_data.groupby(['turbidity'])[['a275', 'a365', 'aVis']].mean()
        return True
    def modify(self):
        ks=get_transfer_data(self.qda_uuid,self.deviceDesc)
        self.transfer_data_mean['a275']-=ks['pw_275'].iloc[0]
        self.transfer_data_mean['a365']-=ks['pw_365'].iloc[0]
        self.transfer_data_mean['aVis']-=ks['pw_Vis'].iloc[0]
        self.transfer_data_mean['a275']*=ks['k_275'].iloc[0]
        self.transfer_data_mean['a365']*=ks['k_365'].iloc[0]
        self.transfer_data_mean['aVis']*=ks['k_Vis'].iloc[0]
        if max(abs(self.transfer_data_mean.iloc[0]))>0.05:
            self.transfer_data_mean['aVis']*=0



def fit_linear_regression(X_, y_):
    """
    计算新设备吸光度和样机吸光度之间的关系
    :param X_: 新设备在纯水和不同浓度梯度浊度液中的吸光度，扣掉该设备纯水吸光度之后的值
    :param y_: 样机设备在纯水和不同浓度梯度浊度液中的吸光度，扣掉样机纯水吸光度之后的值
    :return: 新设备吸光度和样机吸光度之间的换算关系(slope:斜率，r_square:相关系数)
    新增截距判断，如果截距偏离0超过阈值，说明定标过程水质不合格，输出用截距取代r^2，
    否者正常输出斜率和r^2。
    """
    reg = LinearRegression()
    reg.fit(X_, y_)
    if abs(reg.intercept_)>0.025:
        slope = round(reg.coef_[0, 0], 6)
        r_square = round(reg.intercept_[0], 6)
    else:
        reg = LinearRegression(fit_intercept=False)
        reg.fit(X_, y_)
        slope = round(reg.coef_[0, 0], 6)
        r_square = round(reg.score(X_, y_), 6)

    return slope, r_square


def get_fitted_path(device, channel, sectionName, standard_device, is_submit):
    """
    将数据迁移的线性拟合结果绘图保存
    图片命名格式：新设备 to 样机_通道名称_批次名称。png
    """
    if not is_submit:
        state = 'try_'
    else:
        state = ''
    title = device + '_to_' + standard_device + '_' + channel + '.png'
    figure_name = state + device + '_to_' + standard_device + '_' + channel + '_' + sectionName + '.png'
    save_path = os.path.join(path, figure_name)
    return title, figure_name, save_path


def transfer(sectionName, standard_device, is_submit):
    transfer_result = []
    global raw_data_
    raw_data_ = get_all_raw_data(sectionName)

    # 先计算样机的纯水吸光度和在不同浓度梯度浊度液中的吸光度
    standard = TransferEPlus(sectionName, deviceDesc=standard_device, deviceStd=standard_device)
    standard.get_device_raw_data()
    standard.modify()

    devices = query_all_devices(sectionName)

    fitteds = []

    for device in devices:
        # 计算待迁移设备的纯水吸光度和在不同浓度梯度浊度液中的吸光度
        new_device = TransferEPlus(sectionName, deviceDesc=device, deviceStd=standard_device)
        if not new_device.get_device_raw_data():
            continue
        device_info = {'qianyi': sectionName, 'qda_uuid':new_device.qda_uuid,'deviceId': new_device.deviceId, 'deviceDesc': new_device.deviceDesc,
                       'deviceStd': new_device.deviceStd, 'pw_275': round(new_device.pure_water_absorb[0], 6),
                       'pw_365': round(new_device.pure_water_absorb[1], 6),
                       'pw_Vis': round(new_device.pure_water_absorb[2], 6), 'satisfied': True}

        # 待测设备的吸光度数据与样机的吸光度数据做拟合
        merge = pd.merge(new_device.transfer_data_mean, standard.transfer_data_mean, on=['turbidity'])
        merge = merge - merge.iloc[0]  # 减掉纯水基线

        for channel in ['a275', 'a365', 'aVis']:
            new_device_absorb = np.array(merge[channel + '_x']).reshape(-1, 1)
            standard_device_absorb = np.array(merge[channel + '_y']).reshape(-1, 1)
            slope, r_square = fit_linear_regression(new_device_absorb, standard_device_absorb)
            y_hat = new_device_absorb * slope
            labels={'2':'2','3':'5','V':'4'}
            label=labels[channel[1]]
            title, figure_name, save_path = get_fitted_path(device, label, sectionName, standard_device, is_submit)

            # 拟合结果可视化，并保存图片及路径
            fitteds.append([new_device_absorb, standard_device_absorb, y_hat, title, save_path])

            if channel.endswith('275'):
                device_info['k_275'], device_info['r_275'], device_info['plot_275'] = slope, r_square, figure_name

                if (device_info['k_275'] < 0.7) or (device_info['k_275'] > 1.2) or (device_info['r_275'] < 0.99):
                    device_info['satisfied'] = False
            elif channel.endswith('365'):
                device_info['k_365'], device_info['r_365'], device_info['plot_365'] = slope, r_square, figure_name
                if (device_info['k_365'] < 0.7) or (device_info['k_365'] > 1.2) or (device_info['r_365'] < 0.99):
                    device_info['satisfied'] = False
            else:
                device_info['k_Vis'], device_info['r_Vis'], device_info['plot_Vis'] = slope, r_square, figure_name
                if (device_info['k_Vis'] < 0.7) or (device_info['k_Vis'] > 1.2) or (device_info['r_Vis'] < 0.98):
                    device_info['satisfied'] = False

        transfer_result.append(device_info)
    transfer_result_ = json.dumps(transfer_result)
    print(transfer_result_)
    return fitteds, transfer_result_


def plot(fitted_info, is_submit):
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    # 拟合结果可视化，并保存图片及路径
    for fitted in fitted_info:
        plt.scatter(fitted[0], fitted[1])
        plt.plot(fitted[0], fitted[2])
        plt.title(fitted[3])
        plt.savefig(fitted[4])
        plt.close()

    if is_submit:
        try_figures = [figure for figure in os.listdir(path) if figure.startswith('try_')]
        for i in try_figures:
            os.remove(os.path.join(path, i))


def main(data):
    # args = sys.argv[1:]
    # section_name = args[0]
    # standard_device = args[1]
    # is_submit = args[2]
    section_name = data.sectionName
    standard_device = data.deviceStd
    is_submit = data.submit
    fitted_info, result = transfer(section_name, standard_device, is_submit)
    plot(fitted_info, is_submit)
    return result


def test():
    # 适用于EP系列数据迁移
    section_name = 'qianyi_20210826_PS'
    standard_device = 'P03Q2103N00124'
    is_submit = True
    fitted_info = transfer(section_name, standard_device, is_submit)
    plot(fitted_info, is_submit)


if __name__ == '__main__':
    main()
    # test()
