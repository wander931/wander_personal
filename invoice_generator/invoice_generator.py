# -*- coding: utf-8 -*-
"""
需求:
    读取送货明细表，按要求汇总成印刷清单
业务规则：
    0. 过滤条件：客户名称="海南普利制药股份有限公司" and 状态="已审核"
    1. 单张印刷清单的总金额<=10w
    2. 合同编号、单据号、OA单号、SAP单号，四个号的总条数不能超过15条
    3. 一张送货单内的内容，只能出现在一个印刷清单中

数据规则：
    1. 合同编号:OA单号:SAP单号 = 1:1:1
    2. 合同编号:单据号 = 1:1~3
    3. 合同编号:送货单 = n:n，即一个合同可以分布在多个送货单中，一个送货单也可以包含多个合同编号

思路：
    1. 从送货明细表提取出送货单维度的明细，一个送货单一张表，按送货单的金额从大到小排序
    2. 取第0位为主key, 再遍历1以后的送货单，0+1+2+3...直到不满足规则的时候停止循环，取上一组排列，并把刚刚取到的结果(如0+3+4)从送货表剔除
    3. 以此类推，新表从0位开始取，一直往后加
    4. 不满足规则的条件
        1. 总金额99000<x<=10w，偏差1000一档？
        2. 4个号的列表，去重后长度<=15
"""

from __future__ import unicode_literals
import pandas as pd
import time
import os
import sys
import re

dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir)


def data_filter(data_frame):
    '''过滤规则
    1. 客户名称="海南普利制药股份有限公司"
    2. 状态="已审核"
    3. 备注中包含合同编号
    '''
    try:
        len_old =len(data_frame)
        data_frame = data_frame[(data_frame["审核"] == "是")
                              & (data_frame["客户名称"] == "海南普利制药股份有限公司")
                              & (data_frame['工单备注'].str.contains(r".*合同编号.*"))
                    ]
        if len_old-len(data_frame) > 0:
            print("有%s条记录不符合规则(未审核/客户名称不匹配/工单备注没有合同编号)，已被过滤" % (len_old-len(data_frame)))
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
                                      header=1,
                                      usecols=["审核", "工程号", "送货单号", "客户名称", "产品名称", "产品规格", "数量", "单位", "单价",
                                               "金额", "工单备注"],
                                      dtype={"数量": float, "单价": float, "金额": float}
                                      ).dropna(subset=["送货单号"])\
        .fillna({"数量": 0, "单价": 0, "金额": 0}) \
        .fillna("")

    # # 重命名列名
    # delivery_info_raw.rename(columns={'审核': '状态'}, inplace=True)

    # 过滤无效数据：客户名称!=普利的，状态!=已审核的，以及工单备注里不包含合同编号的
    delivery_info_list = data_filter(delivery_info_raw).to_dict(orient="records")

    # 聚合一个以送货单为key的dict，后面用来判断规则用
    info_groupby_deliver_no_dic = {}
    for info in delivery_info_list:
        info_groupby_deliver_no_dic[info["送货单号"]] = {"金额": 0, "单据号": []}

    # 提取工单备注的：合同编号、单据号/计划号、OA单号、SAP单号，增加进data dict
    for i in range(len(delivery_info_list)):
        info = delivery_info_list[i]
        comment = str(info["工单备注"])

        contract_list = re.findall(r'合同编号.+?([0-9、，, ]{5,50})', comment)
        contract_no = get_pure_number_list(contract_list)
        delivery_info_list[i]["合同编号"] = contract_no

        # 单据号在备注里可能叫计划号，这里的(?:计划号|单据号)代表二选一匹配两个完整的词，但是最后选出的是后面括号中的数字；?:用来忽略本身的括号，以避免和后面真正要匹配的字符混淆
        bill_list = re.findall(r'(?:计划号|单据号).+?([0-9、，, ]{5,100})', comment)
        bill_no = get_pure_number_list(bill_list)
        delivery_info_list[i]["单据号"] = bill_no

        OA_list = re.findall(r'OA单号.+?([0-9、，, ]{5,50})', comment)
        OA_no = get_pure_number_list(OA_list)
        delivery_info_list[i]["OA单号"] = OA_no

        SAP_list = re.findall(r'SAP订单号.+?([0-9、，, ]{5,50})', comment)
        SAP_no = get_pure_number_list(SAP_list)
        delivery_info_list[i]["SAP订单号"] = SAP_no

        # 给按送货单分组的dict增加信息
        info_groupby_deliver_no_dic[info["送货单号"]]["金额"] += info["金额"]
        info_groupby_deliver_no_dic[info["送货单号"]]["单据号"] += contract_no
        info_groupby_deliver_no_dic[info["送货单号"]]["单据号"] += bill_no
        info_groupby_deliver_no_dic[info["送货单号"]]["单据号"] += OA_no
        info_groupby_deliver_no_dic[info["送货单号"]]["单据号"] += SAP_no

    # info_groupby_deliver_no_dic转成有序的列表，并按金额从大到小排序
    info_groupby_deliver_no = [{"送货单号": k, "金额": v["金额"], "单据号": v["单据号"]} for k, v in info_groupby_deliver_no_dic.items()]
    info_groupby_deliver_no = sorted(info_groupby_deliver_no, key=lambda d: d['金额'])
    info_groupby_deliver_no.reverse()

    return delivery_info_list, info_groupby_deliver_no


def validate_invoice(info_group):
    """ 校验发票是否成立
    返回True或者False

    规则1: 单张印刷清单的总金额<=9w
    (废弃)规则2. 合同编号、单据号、OA单号、SAP单号，四个号的总条数不能超过15条
    规则3. 一张送货单内的内容，只能出现在一个印刷清单中

    即：
    1. 一个送货单号不能同时出现在多个子列表里
    2. group list中，每个子列表的总金额<=9w
    否则返回false
    """
    invoice_total_gmv = 0
    invoice_total_bill_no = []
    for i in info_group:
        invoice_total_gmv += float(i["金额"])
        invoice_total_bill_no += list(i["单据号"])

    comment_length = 0
    for i in set(invoice_total_bill_no):
        comment_length += len(i)+1

    # if invoice_total_gmv > 90000 or len(set(invoice_total_bill_no)) > 15:
    # if invoice_total_gmv > 90000:
    # 新增需求：备注不得超过156字符。comment有固定24个字符+4个换行符，这里判断单据号总长度不超过156-24=132字符即可。如果某个单据号为空则可能会出现1字符(换行符)的偏差
    if invoice_total_gmv>90000 or comment_length>132:
        return False
    else:
        return True


def get_valid_group(info_groupby_deliver_no):
    """
    1. 从送货明细表提取出送货单维度的明细，一个送货单一张表，按送货单的金额从大到小排序
    2. 取第0位为主key, 再遍历1以后的送货单，0+1+2+3...直到不满足规则的时候停止循环，取上一组排列，并把刚刚取到的结果(如0+3+4)从送货表剔除
    3. 以此类推，新表从0位开始取，一直往后加
    4. 不满足规则的条件
        1. 总金额99000<x<=10w，偏差1000一档？
        2. 4个号的列表，去重后长度<=15
    """
    invoice_groups = []
    # print("info_groupby_deliver_no", info_groupby_deliver_no)

    while info_groupby_deliver_no:
        # 先把第0位取出来，同时在原列表中把该元素删掉
        group = [info_groupby_deliver_no[0]]
        info_groupby_deliver_no.remove(group[0])
        # 遍历删掉0位后的列表，满足条件的元素也从里面删掉
        for i in info_groupby_deliver_no[:]:
            if validate_invoice(group + [i]):
                group += [i]
                info_groupby_deliver_no.remove(i)
        invoice_groups.append(group)
    return invoice_groups


def main(data_file, result_file):
    delivery_info_list, info_groupby_deliver_no = get_delivery_info(data_file)

    invoice_groups = get_valid_group(info_groupby_deliver_no)
    invoice_dataframe_list = []
    # 每个元素是一张发票，每张发票中包含多个单据号[{"单据号"：1}, {"单据号"：2}]
    for invoice in invoice_groups:
        final_result_list = []
        contract_no = []
        bill_no = []
        OA_no = []
        SAP_no = []
        for each in invoice:
            deliver_no = each["送货单号"]
            # 根据单据号从原送货明细表中取明细，写进final result中
            for info in delivery_info_list:
                if info["送货单号"] == deliver_no:
                    tmp = {"工程号": info["工程号"],
                           "品名": info["产品名称"],
                           "规格": info["产品规格"],
                           "数量": info["数量"],
                           "单位": info["单位"],
                           "单价(元)": info["单价"],
                           "金额(元)": info["金额"],
                           "备注": ""
                           }
                    final_result_list.append(tmp)
                    contract_no += info["合同编号"]
                    bill_no += info["单据号"]
                    OA_no += info["OA单号"]
                    SAP_no += info["SAP订单号"]
            comment = """合同编号：
%s
单据号：
%s
OA单号：
%s
SAP订单号：
%s""" % ("\n".join(str(i) for i in set(contract_no)),
         "\n".join(str(i) for i in set(bill_no)),
         "\n".join(str(i) for i in set(OA_no)),
         "\n".join(str(i) for i in set(SAP_no)),
         )
        final_result_list[0]["备注"] = comment
        invoice_dataframe_list.append(pd.DataFrame(final_result_list))

    with pd.ExcelWriter(result_file) as writer:
        workbook = writer.book
        # format详细文档见 https://xlsxwriter.readthedocs.io/format.html
        merge_format = workbook.add_format({
            'border': 1,
            'align': 'center',
            'valign': 'top'})
        # 遍历不同的发票sheet
        for idx in range(len(invoice_dataframe_list)):
            df = invoice_dataframe_list[idx]
            sheet_name = '发票_%s' % idx
            worksheet = workbook.add_worksheet(sheet_name)
            writer.sheets[sheet_name] = worksheet
            # 写数据进对应的sheet
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            # 合并备注列，第二个参数是合并后取哪个单元格的内容
            worksheet.merge_range('H2:H35', df.loc[0, '备注'], merge_format)
            # 设置列宽
            worksheet.set_column('B:B', 40)  # 品名
            worksheet.set_column('H:H', 20)

            # # 自动调整宽度
            # for column in df:
            #     column_length = max(df[column].astype(str).map(len).max(), len(column))
            #     col_idx = df.columns.get_loc(column)
            #     writer.sheets[sheet_name].set_column(col_idx, col_idx, column_length)


if __name__ == "__main__":
    now = time.strftime('%Y%m%d_%H_%M_%S', time.localtime(int(time.time())))

    data_file = dir + r'/销售对账明细.xlsx'
    # result_file = dir + r'/印刷清单_%s.xlsx' % now
    result_file = dir + r'/开票明细清单_%s.xlsx' % now

    main(data_file, result_file)
    print("\n计算完成，结果见文件【%s】\n弹窗1分钟后自动关闭，也可手动关闭~" % result_file)
    time.sleep(60)