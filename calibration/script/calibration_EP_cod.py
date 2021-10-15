# -*- coding: utf-8 -*-
"""
@author: syzhao
@created time: 2020/8/28 10:10

@Project: calibration auto
@脚本功能：EP COD率定校准系数
输入：设备数据、样本真值
输出：校准结果
"""

import json
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import TheilSenRegressor


def plana_cod(input_data):
    # plana_cod率定自动化
    df = pd.read_json(json.dumps(input_data))
    # 根据测试人员勾选的水样数据，计算率定参数模型slope，以及率定后的结果fitted_cod
    slope, y_hat = plana_cod_calibration(df)

    df["fitted_cod"] = y_hat
    df = df.round({"fitted_cod": 2}) # 保留两位小数

    is_accurate = []
    for i in range(len(df)):
        if df["sample_cod"][i] <= 30 and abs(df["sample_cod"][i] - df["fitted_cod"][i]) <= 6:
            # 若cod在30以下，如果误差小于6，则认为率定结果达标
            is_accurate.append(1)
        elif df["sample_cod"][i] > 30 and abs(df["sample_cod"][i] - df["fitted_cod"][i]) <= 0.2 * df["sample_cod"][i]:
            # 若COD在30以上，如果相对误差小于20%，则认为率定结果达标
            is_accurate.append(1)
        else:
            # 否则认为率定结果不达标
            is_accurate.append(0)
    df["is_accurate"] = is_accurate

    output = {}

    if len(df[df["is_accurate"] == 0]) >= len(df[df["is_accurate"] == 1]):
        output["allow_submit"] = 0
    else:
        # 如果达标率大于50%，则可以提交参数模型，反之则不能提交
        output["allow_submit"] = 1
    output["show_content"] = df[["sample_sectionName", "deviceDesc", "sample_cod", "device_cod", "fitted_cod", "is_accurate"]].to_dict(orient='records')
    
    # 返回要插入到数据库中的参数模型给软件
    insert_content = {"section_name": df["sample_sectionName"][0], "section_id": df["sample_sectionCode"][0],
                      "k_a": slope.tolist(), "k_turbidity": np.round(-slope*0.00636, 6).tolist(), "w": 0.0, "b": 0.0}
    output["insert_content"] = insert_content
    return json.dumps(output, ensure_ascii=False)
    

def ep_cod(input_data):
    # planEP_cod率定自动化
    df = pd.read_json(json.dumps(input_data))
    slope, y_hat = ep_cod_calibration(df)

    df["fitted_cod"] = y_hat
    df = df.round({"fitted_cod": 2})

    is_accurate = []
    for i in range(len(df)):
        if df["sample_cod"][i] <= 30 and abs(df["sample_cod"][i] - df["fitted_cod"][i]) <= 6:
            is_accurate.append(1)
        elif df["sample_cod"][i] > 30 and abs(df["sample_cod"][i] - df["fitted_cod"][i]) <= 0.2 * df["sample_cod"][i]:
            is_accurate.append(1)
        else:
            is_accurate.append(0)
    df["is_accurate"] = is_accurate

    output = {}

    if len(df[df["is_accurate"] == 0]) >= len(df[df["is_accurate"] == 1]):
        output["allow_submit"] = 0
    else:
        output["allow_submit"] = 1
    output["show_content"] = df[["sample_sectionName", "deviceDesc", "sample_cod", "device_cod", "fitted_cod", "is_accurate"]].to_dict(orient='records')

    insert_content = {"section_name": df["sample_sectionName"][0], "section_id": df["sample_sectionCode"][0],
                      "k2_a275": 0, "k_a275": slope, "k2_a365": 0.0, "k_a365": 0.0,
                      "k2_aVis": 0.0, "k_aVis": 0.0, "b": 0.0}
    output["insert_content"] = insert_content
    return json.dumps(output, ensure_ascii=False)
    

def epps_cod(input_data):
    # planEPPs_cod率定自动化
    df = pd.read_json(json.dumps(input_data))
    slope, y_hat = epps_cod_calibration(df)

    df["fitted_cod"] = y_hat
    df = df.round({"fitted_cod": 2})

    is_accurate = []
    for i in range(len(df)):
        if df["sample_cod"][i] <= 30 and abs(df["sample_cod"][i] - df["fitted_cod"][i]) <= 6:
            is_accurate.append(1)
        elif df["sample_cod"][i] > 30 and abs(df["sample_cod"][i] - df["fitted_cod"][i]) <= 0.2 * df["sample_cod"][i]:
            is_accurate.append(1)
        else:
            is_accurate.append(0)
    df["is_accurate"] = is_accurate

    output = {}

    if len(df[df["is_accurate"] == 0]) >= len(df[df["is_accurate"] == 1]):
        output["allow_submit"] = 0
    else:
        output["allow_submit"] = 1
    output["show_content"] = df[["sample_sectionName", "deviceDesc", "sample_cod", "device_cod", "fitted_cod", "is_accurate"]].to_dict(orient='records')

    insert_content = {"section_name": df["sample_sectionName"][0], "section_id": df["sample_sectionCode"][0],
                      "k2_a275": 0, "k_a275": slope, "k2_a365": 0.0, "k_a365": 0.0,
                      "k2_aVis": 0.0, "k_aVis": 0.0, "b": 0.0}
    output["insert_content"] = insert_content
    return json.dumps(output, ensure_ascii=False)


def ep_turb(input_data):
    # planEP_浊度率定自动化
    df = pd.read_json(json.dumps(input_data))
    slope, y_hat = ep_turb_calibration(df)

    df["fitted_turb"] = y_hat
    df = df.round({"fitted_turb": 2})

    is_accurate = []
    for i in range(len(df)):
        if abs(df["sample_turb"][i] - df["fitted_turb"][i]) <= 0.2 * df["sample_turb"][i]:
            is_accurate.append(1)
        else:
            is_accurate.append(0)
    df["is_accurate"] = is_accurate

    output = {}

    if len(df[df["is_accurate"] == 0]) >= len(df[df["is_accurate"] == 1]):
        output["allow_submit"] = 0
    else:
        output["allow_submit"] = 1
    output["show_content"] = df[["sample_sectionName", "deviceDesc", "sample_turb", "device_turb", "fitted_turb", "is_accurate"]].to_dict(orient='records')

    insert_content = {"section_name": df["sample_sectionName"][0], "section_id": df["sample_sectionCode"][0],
                      "k2_a275": 0, "k_a275": slope, "k2_a365": 0.0, "k_a365": 0.0,
                      "k2_aVis": 0.0, "k_aVis": 0.0, "b": 0.0}
    output["insert_content"] = insert_content
    return json.dumps(output, ensure_ascii=False)


def plana_cod_calibration(df):
    """planA_cod率定函数"""
    y_ = np.array(df["sample_cod"])
    x_ = np.array(df["a"]).reshape(-1, 1) - 0.00636 * np.array(df["device_turb"]).reshape(-1, 1)

    coef = fit_theilsen_regression(x_, y_)
    coef = np.round(coef, 6)
    # 将率定参数模型系数设定在[40, 120]
    if coef > 120:
        coef = 120
    elif coef < 40:
        coef = 40
    y_hat = x_ * coef

    return coef, y_hat

def ep_cod_calibration(df):
    """planEP_cod率定函数"""
    y_ = np.array(df["sample_cod"])
    x_ = np.array(df["a275"]).reshape(-1, 1)
    coef = fit_theilsen_regression(x_, y_)
    coef = round(coef, 6)
    # 将率定参数模型系数设定在[30, 120]
    if coef > 120:
        coef = 120
    elif coef < 30:
        coef = 30
    y_hat = x_ * coef

    return coef, y_hat


def epps_cod_calibration(df):
    """planEPPs_cod率定函数"""
    y_ = np.array(df["sample_cod"])
    x_ = np.array(df["a275"]).reshape(-1, 1)
    coef = fit_theilsen_regression(x_, y_)
    coef = round(coef, 6)
    # 设定率定参数大于200, 管井COD变化较大，不设上限
    if coef < 200:
        coef = 200
    y_hat = x_ * coef
    
    return coef, y_hat


def ep_turb_calibration(df):
    """planEP_浊度率定函数"""
    y_ = np.array(df["sample_turb"])
    x_ = np.array(df["a365"]).reshape(-1, 1)
    coef = fit_theilsen_regression(x_, y_)
    coef = round(coef, 6)
    # 将率定参数模型系数设定在[55, 100]
    if coef > 100:
        coef = 100
    elif coef < 55:
        coef = 55
    y_hat = x_ * coef

    return coef, y_hat


def fit_linear_regression(x_, y_):
    """
    计算设备吸光度和指标之间的关系
    :param x_: 设备在样本中的吸光度，是做过数据迁移后的数值
    :param y_: 水样指标真值
    :return: 新计算设备吸光度和指标之间的计算系数(slope:斜率，r_square:相关系数)
    """
    reg = LinearRegression(fit_intercept=False)
    reg.fit(x_, y_)
    slope = reg.coef_[0, 0]
    # r_square = reg.score(x_, y_)
    return slope


def fit_theilsen_regression(x_, y_):
    """
    计算设备吸光度和指标之间的关系，相对于linear_regression, 对异常值不敏感，更稳定
    :param x_: 设备在样本中的吸光度，是做过数据迁移后的数值
    :param y_: 水样指标真值
    :return: 新计算设备吸光度和指标之间的计算系数(coef_:拟合系数)
    """
    reg = TheilSenRegressor(fit_intercept=False)
    reg.fit(x_, y_)

    return reg.coef_
