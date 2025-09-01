# -*- coding: utf-8 -*-
"""
需求:
    读取送货明细表，按要求汇总成印刷清单
    
** 重要更新：2024年需求变更 **
原需求：一张送货单内的内容，只能出现在一个印刷清单中
新需求：一个合同编号内的内容，只能出现在一个印刷清单中

业务规则：
    0. 过滤条件：客户名称="海南普利制药股份有限公司" and 状态="已审核"
    1. 单张印刷清单的总金额<=9w（90000元）
    2. 备注字符长度<=176字符
    3. 一个合同编号内的内容，只能出现在一个印刷清单中（新规则）

数据规则：
    1. 合同编号:OA单号:SAP单号 = 1:1:1
    2. 合同编号:单据号 = 1:1~3
    3. 合同编号:送货单 = n:n，即一个合同可以分布在多个送货单中，一个送货单也可以包含多个合同编号

思路（已更新）：
    1. 从送货明细表提取出合同编号维度的明细，一个合同编号一个分组，按合同金额从大到小排序
    2. 取第0位为主key, 再遍历1以后的合同，0+1+2+3...直到不满足规则的时候停止循环，取上一组排列，并把刚刚取到的结果从合同表剔除
    3. 以此类推，新表从0位开始取，一直往后加
    4. 不满足规则的条件
        1. 总金额>90000元
        2. 备注字符长度>176字符
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
                              & (data_frame['工单备注'].str.contains(r"合同编号"))
                              & (data_frame['工单备注'].str.contains(r"单据号|计划单号"))
                    ]
        if len_old-len(data_frame) > 0:
            print("有%s条记录不符合规则(未审核/客户名称不匹配/工单备注缺少合同编号或单据号)，已被过滤" % (len_old-len(data_frame)))
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

    # 提取工单备注的：合同编号、单据号/计划号、OA单号、SAP单号，增加进data dict
    for i in range(len(delivery_info_list)):
        info = delivery_info_list[i]
        comment = str(info["工单备注"])

        contract_list = re.findall(r'合同编号.+?([0-9、，, -]{5,50})', comment)
        contract_no = get_pure_number_list(contract_list)
        delivery_info_list[i]["合同编号"] = contract_no

        # 单据号在备注里可能叫计划号，这里的(?:计划号|单据号)代表二选一匹配两个完整的词，但是最后选出的是后面括号中的数字；?:用来忽略本身的括号，以避免和后面真正要匹配的字符混淆
        bill_list = re.findall(r'(?:计划号|单据号|计划单号).+?([0-9、，, -]{5,100})', comment)
        bill_no = get_pure_number_list(bill_list)
        delivery_info_list[i]["单据号"] = bill_no

        OA_list = re.findall(r'OA单号.+?([0-9、，, -]{5,50})', comment)
        OA_no = get_pure_number_list(OA_list)
        delivery_info_list[i]["OA单号"] = OA_no

        SAP_list = re.findall(r'SAP订单号.+?([0-9、，, -]{5,50})', comment)
        SAP_no = get_pure_number_list(SAP_list)
        delivery_info_list[i]["SAP订单号"] = SAP_no

    # 按合同编号分组（新需求：一个合同编号在一张发票里）
    info_groupby_contract_no = get_contract_groups(delivery_info_list)

    return delivery_info_list, info_groupby_contract_no


def get_contract_groups(delivery_info_list):
    """
    按合同编号分组
    新需求：一个合同编号的所有内容必须在同一张发票中
    """
    # 聚合一个以合同编号为key的dict
    info_groupby_contract_dic = {}
    
    for info in delivery_info_list:
        contract_numbers = info.get("合同编号", [])
        
        # 如果没有合同编号，跳过（这种情况应该在过滤阶段就被排除了）
        if not contract_numbers:
            print("警告：送货单 %s 没有合同编号，已跳过" % info['送货单号'])
            continue
            
        # 一个记录可能有多个合同编号，但通常只有一个
        for contract_no in contract_numbers:
            if contract_no not in info_groupby_contract_dic:
                info_groupby_contract_dic[contract_no] = {
                    "金额": 0, 
                    "单据号": [],
                    "送货单号": [],
                    "明细数": 0
                }
            
            # 累加金额
            info_groupby_contract_dic[contract_no]["金额"] += info["金额"]
            
            # 收集所有单据号
            info_groupby_contract_dic[contract_no]["单据号"] += info.get("合同编号", [])
            info_groupby_contract_dic[contract_no]["单据号"] += info.get("单据号", [])
            info_groupby_contract_dic[contract_no]["单据号"] += info.get("OA单号", [])
            info_groupby_contract_dic[contract_no]["单据号"] += info.get("SAP订单号", [])
            
            # 收集送货单号
            if info["送货单号"] not in info_groupby_contract_dic[contract_no]["送货单号"]:
                info_groupby_contract_dic[contract_no]["送货单号"].append(info["送货单号"])
            
            # 记录明细条数
            info_groupby_contract_dic[contract_no]["明细数"] += 1

    # 转成有序的列表，并按金额从大到小排序
    info_groupby_contract_no = [
        {
            "合同编号": k, 
            "金额": v["金额"], 
            "单据号": v["单据号"],
            "送货单号": v["送货单号"],
            "明细数": v["明细数"]
        } 
        for k, v in info_groupby_contract_dic.items()
    ]
    
    # 按金额从大到小排序
    info_groupby_contract_no = sorted(info_groupby_contract_no, key=lambda d: d['金额'], reverse=True)
    
    print("按合同编号分组完成，共 %s 个合同" % len(info_groupby_contract_no))
    
    return info_groupby_contract_no


def validate_invoice(info_group):
    """ 校验发票是否成立
    返回True或者False

    规则1: 单张印刷清单的总金额<=9w
    (废弃)规则2. 合同编号、单据号、OA单号、SAP单号，四个号的总条数不能超过15条
    规则3. 一个合同编号内的内容，只能出现在一个印刷清单中（新需求）

    即：
    1. 一个合同编号不能同时出现在多个子列表里
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
    # 新增需求：备注不得超过200字符。comment有固定24个字符+4个换行符，这里判断单据号总长度不超过200-24=176字符即可。如果某个单据号为空则可能会出现1字符(换行符)的偏差
    if invoice_total_gmv>90000 or comment_length>176:
        return False
    else:
        return True


def get_valid_group(info_groupby_contract_no):
    """
    按合同编号分组生成发票（新需求）
    1. 从送货明细表提取出合同编号维度的明细，一个合同编号一个分组，按合同金额从大到小排序
    2. 取第0位为主key, 再遍历1以后的合同，0+1+2+3...直到不满足规则的时候停止循环，取上一组排列，并把刚刚取到的结果(如0+3+4)从合同表剔除
    3. 以此类推，新表从0位开始取，一直往后加
    4. 不满足规则的条件
        1. 总金额<=90000元
        2. 备注字符长度<=176字符
    """
    invoice_groups = []
    over_limit_count = 0  # 统计超限发票数量
    # print("开始分组，共 %s 个合同需要处理" % len(info_groupby_contract_no))

    while info_groupby_contract_no:
        # 先把第0位取出来，同时在原列表中把该元素删掉
        base_contract = info_groupby_contract_no[0]
        info_groupby_contract_no.remove(base_contract)
        
        # 检查单个合同是否超过限制
        is_over_limit = not validate_invoice([base_contract])
        if is_over_limit:
            over_limit_count += 1
            print("⚠️  警告：合同编号 %s 金额 %s 超过单张发票限制(90000元)，但仍将单独开票" % (base_contract['合同编号'], base_contract['金额']))
        
        group = [base_contract]
        # print("新发票基础：合同编号 %s，金额 %s" % (group[0]['合同编号'], group[0]['金额']))
        
        # 遍历删掉0位后的列表，满足条件的元素也从里面删掉
        for i in info_groupby_contract_no[:]:
            if validate_invoice(group + [i]):
                # print("  ✅ 加入合同编号 %s，金额 %s" % (i['合同编号'], i['金额']))
                group += [i]
                info_groupby_contract_no.remove(i)
            else:
                # print("  ❌ 跳过合同编号 %s，金额 %s (超出限制)" % (i['合同编号'], i['金额']))
                pass
        
        invoice_groups.append(group)
        
        # 计算当前发票总金额
        total_amount = sum(contract["金额"] for contract in group)
        contract_count = len(group)
        print("完成发票 %s：包含 %s 个合同，总金额 %s" % (len(invoice_groups), contract_count, total_amount))
        print("-" * 50)
    
    print("分组完成！共生成 %s 张发票" % len(invoice_groups))
    
    # 显示超限统计
    if over_limit_count > 0:
        print("⚠️  注意：有 %s 张发票超过90000元限制" % over_limit_count)
        print("   这些发票对应的是单个大额合同，无法进一步拆分")
        print("   请检查业务流程或考虑调整发票限额")
    else:
        print("✅ 所有发票都符合90000元限制")
    
    return invoice_groups


def main(data_file, result_file):
    delivery_info_list, info_groupby_contract_no = get_delivery_info(data_file)

    invoice_groups = get_valid_group(info_groupby_contract_no)
    invoice_dataframe_list = []
    # 每个元素是一张发票，每张发票中包含多个合同编号
    for invoice in invoice_groups:
        final_result_list = []
        contract_no = []
        bill_no = []
        OA_no = []
        SAP_no = []
        
        # 收集当前发票中所有合同编号涉及的送货单
        all_delivery_nos = []
        for each in invoice:
            all_delivery_nos.extend(each["送货单号"])
        
        # 根据送货单号从原送货明细表中取明细，写进final result中
        for deliver_no in set(all_delivery_nos):  # 去重
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
        
        # 生成备注
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
        
        if final_result_list:  # 确保有数据才设置备注
            final_result_list[0]["备注"] = comment
            invoice_dataframe_list.append(pd.DataFrame(final_result_list))

    with pd.ExcelWriter(result_file) as writer:
        workbook = writer.book
        # format详细文档见 https://xlsxwriter.readthedocs.io/format.html
        merge_format = workbook.add_format({
            'border': 1,
            'align': 'center',
            'valign': 'top',
            'text_wrap': True  # 启用文本换行，让换行符生效
        })
        # 遍历不同的发票sheet
        for idx in range(len(invoice_dataframe_list)):
            df = invoice_dataframe_list[idx]
            sheet_name = '发票_%s' % idx
            worksheet = workbook.add_worksheet(sheet_name)
            writer.sheets[sheet_name] = worksheet
            # 写数据进对应的sheet
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            # 合并备注列，第二个参数是合并后取哪个单元格的内容
            worksheet.merge_range('H2:H20', df.loc[0, '备注'], merge_format)
            
            # 设置列宽
            worksheet.set_column('B:B', 40)  # 品名
            worksheet.set_column('H:H', 25)  # 备注列稍微宽一些
            
            # 设置合适的行高以显示备注内容
            # 计算备注行数和所需高度
            comment_text = df.loc[0, '备注']
            comment_lines = comment_text.count('\n') + 1  # 计算备注行数
            
            # 为所有数据行设置统一的行高，确保备注完整显示
            # 备注合并单元格H2:H20会占用多行，每行都需要适当高度
            standard_row_height = max(20, 120 // min(len(df), 19))  # 根据数据行数调整行高
            
            # 为数据行设置行高（从第2行开始，因为第1行是表头）
            data_rows = len(df)
            for row_num in range(1, min(data_rows + 1, 20)):  # 最多到第20行
                worksheet.set_row(row_num, standard_row_height)


if __name__ == "__main__":
    now = time.strftime('%Y%m%d_%H_%M_%S', time.localtime(int(time.time())))

    data_file = dir + r'/销售对账明细.xlsx'
    # result_file = dir + r'/印刷清单_%s.xlsx' % now
    result_file = dir + r'/开票明细清单_%s.xlsx' % now

    main(data_file, result_file)
    print("\n计算完成，结果见文件【%s】\n弹窗1分钟后自动关闭，也可手动关闭~" % result_file)
    time.sleep(60)