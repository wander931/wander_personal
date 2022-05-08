# -*- coding: utf-8 -*-
"""
需求:
    读取送货明细表，按要求汇总成印刷清单
业务规则：
    0. 过滤条件：客户名称="海南普利制药股份有限公司" and 状态="已审核"
    1. 单张印刷清单的总金额<=10w
    2. 合同编号、单据号、OA单号、SAP单号，四个号的总条数不能超过16条
    3. 一张送货单内的内容，只能出现在一个印刷清单中

数据规则：
    1. 合同编号:OA单号:SAP单号 = 1:1:1
    2. 合同编号:单据号 = 1:1~3
    3. 合同编号:送货单 = n:n，即一个合同可以分布在多个送货单中，一个送货单也可以包含多个合同编号

思路：
    1. 计算sum(金额)，判断最少要有多少张发票，发票数n>=ceil(sum(gmv))，即最少要组合成n张发票
    2. for i in range(n,明细条数), 分析目标list切成i片的全部场景，遍历完组合出x种场景
    3. 判断不符合规则的场景
        1. 总金额<=10w
        2. 4个号的列表，去重后长度<=16
    4. 按切片个数从小到大排，取第一种可能


            合同号     单据号     OA号     SAP号       金额
=========================================================
送货单①        A        A1         A        A         5w
              A        A2         A        A
              B        B1         B        B
=========================================================
送货单②        A        A3         A        A         4w
              C        C1         C        C
              C        C2         C        C
=========================================================
送货单③        B        B2        B         B         2w
              C        C3        C         C


测试excel中，一共8个送货单(单据号这一列是送货单)，17条明细, 其中1条未审核


"""

from __future__ import unicode_literals
import pandas as pd
import numpy as np
import math
import time
import os
import sys
import re
import traceback
from more_itertools import set_partitions

dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir)


def data_filter(data_frame):
    '''过滤规则
    1. 客户名称="海南普利制药股份有限公司"
    2. 状态="已审核"
    3. 备注中包含合同编号
    '''
    try:
        data_frame = data_frame[(data_frame["状态"] == "已审核")
                              & (data_frame["客户名称"] == "海南普利制药股份有限公司")
                              & (data_frame['工单备注'].str.contains(r".*合同编号.*"))
                    ]
    except:
        pass
    finally:
        return data_frame


def get_pure_number_list(raw_str_list):
    """单号填写不规范，可能会存在提取到包含、，, 的内容，或者一个字符串包含多个单号，如"2240002224、220002274"，"2240002224, 220002274"
    需要先按、，,分割，然后去掉单号首尾的空格, 去掉空字符串，最后转成set去重
    """
    pure_number_list = []

    if raw_str_list:
        for raw_str in raw_str_list:
            tmp = raw_str.replace(",", "、").replace("，", "、").split("、")
            for i in tmp:
                if i:   # 空字符串则忽略
                    pure_number_list.append(i)

    return list(set(pure_number_list))


def get_delivery_info(data_file):
    # 默认读第一个sheet, header=0代表从第1行开始读, 读取指定列; 为空时指定字段用0填充，其他字段为空用""填充；最后转成key:value list
    delivery_info_raw = pd.read_excel(data_file,
                                      # header=0,
                                      usecols=["状态 ", "工程号", "单据号", "客户名称", "产品名称", "产品规格", "数量", "单位", "单价(本位币)",
                                               "金额(本位币)", "工单备注"],
                                      dtype={"数量": float, "单价(本位币)": float, "金额(本位币)": float}
                                      ).dropna(subset=["单据号"])\
        .fillna({"数量": 0, "单价(本位币)": 0, "金额(本位币)": 0}) \
        .fillna("")

    # 重命名两列
    delivery_info_raw.rename(columns={'状态 ': '状态', '单据号': '送货单号'}, inplace=True) \

    # 过滤无效数据：客户名称!=普利的，状态!=已审核的，以及工单备注里不包含合同编号的
    delivery_info_dict = data_filter(delivery_info_raw).to_dict(orient="records")
    # print(delivery_info_raw)
    # print(delivery_info_dict)

    # 提取工单备注的：合同编号、单据号/计划号、OA单号、SAP单号，增加进data dict
    for i in range(len(delivery_info_dict)):
        info = delivery_info_dict[i]
        comment = str(info["工单备注"])

        contract_list = re.findall(r'合同编号.+?([0-9、，, ]{5,25})', comment)
        contract_no = get_pure_number_list(contract_list)
        delivery_info_dict[i]["合同编号"] = contract_no

        # 单据号在备注里可能叫计划号，这里的(?:计划号|单据号)代表二选一匹配两个完整的词，但是最后选出的是后面括号中的数字；?:用来忽略本身的括号，以避免和后面真正要匹配的字符混淆
        bill_list = re.findall(r'(?:计划号|单据号).+?([0-9、，, ]{5,25})', comment)
        bill_no = get_pure_number_list(bill_list)
        delivery_info_dict[i]["单据号"] = bill_no

        OA_list = re.findall(r'OA单号.+?([0-9、，, ]{5,25})', comment)
        OA_no = get_pure_number_list(OA_list)
        delivery_info_dict[i]["OA单号"] = OA_no

        SAP_list = re.findall(r'SAP订单号.+?([0-9、，, ]{5,25})', comment)
        SAP_no = get_pure_number_list(SAP_list)
        delivery_info_dict[i]["SAP订单号"] = SAP_no

    return delivery_info_dict


def main(data_file, result_file):
    delivery_info_dict = get_delivery_info(data_file)
    print(delivery_info_dict)

    # final_result = pd.DataFrame(final_result_list)
    # grouped = final_result.groupby("姓名").agg({"补贴金额": "sum"})
    #
    # with pd.ExcelWriter(result_file) as writer:
    #     final_result.to_excel(writer, sheet_name='补贴明细', index=False)
    #     grouped.to_excel(writer, sheet_name='金额合计')
    #


def debug():
    # x = []
    # min_partitions = 3
    # max_partitions = 10
    # for partition in range(1,11):
    #     x += list(set_partitions([i for i in range(10)], k=partition))
    #
    # print(x)
    # print(len(x))
    test = """（125*52*60mm）盒子的物料编号:80011443;合同编号:2022021610503;单据号:2240002224、220002274;OA单号:2150003098 ; SAP订单号:4100006586。（115*52*60mm）盒子物料编号:10001517;合同编号:2022021610503;计划号:3240002224，320002274;OA单号:2150003098 ; SAP订单号:4100006586。。"""
    contract_no = re.findall(r'(?:计划号|单据号).+?([0-9、，, ]{5,25})', test)
    # contract_no = re.findall(r'合同编号.+?([0-9、，, ]{5,25})', test)
    print(contract_no)
    # if contract_no:  # 语法错误，不重试
    #     contract_no = contract_no.group(0).split("\n")[:10])  # 取报错日志前10行


if __name__ == "__main__":
    now = time.strftime('%Y%m%d_%H_%M_%S', time.localtime(int(time.time())))

    data_file = dir + r'/送货单明细_test.xlsx'
    result_file = dir + r'/印刷清单_%s.xlsx' % now

    main(data_file, result_file)
    # debug()