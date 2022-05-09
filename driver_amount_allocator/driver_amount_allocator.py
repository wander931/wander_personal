# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import pandas as pd
import numpy as np
import math
import time
import os
import sys
import traceback

dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir)


def check_client_name(rule, name):
    rule = str(rule).strip()
    if rule == "":
        if "葫芦娃" not in str(name):
            return True
    elif str(rule) in str(name):
        return True
    else:
        return False


def check_back_car(rule, car):
    rule = str(rule).strip()
    if rule == "":
        if str(car) == "":
            return True
    elif str(rule) in str(car):
        return True
    else:
        return False


def check_weight(rule, weight):
    rule = str(rule).strip()

    if rule == "=0" and float(weight) <= 0:
        return True
    elif rule == "<2" and 0 < float(weight) < 2:
        return True
    elif rule == ">=2" and float(weight) > 2:
        return True
    else:
        return False


def check_driver2(rule, driver):
    rule = str(rule).strip()
    if rule == "":
        if str(driver) == "" or str(driver) == "0":
            return True
    elif rule == "有":
        if str(driver) != "" and str(driver) != "0":
            return True
    else:
        return False


def amount_allocate(bill_no, driver_amount, driver2_amount, driver, driver2=None, assis1=None, assis2=None,
                    assis3=None, assis4=None, assis5=None, assis6=None):
    # 参数类型/非空校验
    if not bill_no or not driver:
        raise ValueError("Excel内容异常：单据号/驾驶员不能为空！")

    if not driver_amount:
        raise ValueError("Excel内容异常：请检查单据号【%s】的场景是否存在，当前场景计算出的补贴金额=0！" % bill_no)

    try:
        driver_amount = float(driver_amount)
        driver2_amount = float(driver2_amount)
    except:
        raise TypeError("Error input(bill_no %s): driver_amount or driver2_amount should be decimal!" % bill_no)

    driver_allocate_roles = {}
    if driver:
        driver_allocate_roles["驾驶员"] = driver
    if assis1:
        driver_allocate_roles["跟车员1"] = assis1
    if assis2:
        driver_allocate_roles["跟车员2"] = assis2
    if assis3:
        driver_allocate_roles["跟车员3"] = assis3
    if assis4:
        driver_allocate_roles["跟车员4"] = assis4
    if assis5:
        driver_allocate_roles["跟车员5"] = assis5

    allocate_result = []
    # 计算平摊的金额，除不尽的向下取整保留两位小数；最后用总金额减掉已分摊金额分给driver，避免总补贴和按人加总对不上
    driver_amount_each = math.floor(driver_amount * 100 / len(driver_allocate_roles)) / 100
    # 先处理跟车员1-5的补贴
    driver_allocated_amount = 0
    for k, v in driver_allocate_roles.items():
        if k != "驾驶员":
            tmp = {
                "单据号": bill_no,
                "角色": k,
                "姓名": v,
                "补贴金额": driver_amount_each,
            }
            allocate_result.append(tmp)
            driver_allocated_amount += driver_amount_each
    # 最后拿总补贴-分给跟车员的补贴，分给驾驶员
    allocate_result.append({
        "单据号": bill_no,
        "角色": "驾驶员",
        "姓名": driver,
        "补贴金额": driver_amount - driver_allocated_amount
    })

    # 当驾驶员2非空且补贴金额>0时，计算驾驶员2的平摊金额
    if driver2 and driver2_amount > 0:
        if assis6:
            allocate_result.append({
                "单据号": bill_no,
                "角色": "跟车员6",
                "姓名": assis6,
                "补贴金额": math.floor(driver2_amount * 100 / 2) / 100
            })
            allocate_result.append({
                "单据号": bill_no,
                "角色": "驾驶员2",
                "姓名": driver2,
                "补贴金额": driver2_amount - math.floor(driver2_amount * 100 / 2) / 100
            })
        else:
            allocate_result.append({
                "单据号": bill_no,
                "角色": "驾驶员2",
                "姓名": driver2,
                "补贴金额": driver2_amount
            })

    return allocate_result


def data_filter_deduplicate(data_frame):
    '''一张单据号存在多条明细时，只取一条；
    一张单据号的客户名称可能同时包含葫芦娃和非葫芦娃，此时需要按葫芦娃统计

    1. 客户名称处理：映射一个新列tmp，葫芦娃->1, 没有葫芦娃->0
                update 客户名称 where 单据号 in ( select 单据号 group by bill_no having sum(tmp)>=1 )
    2. 整表去重
    3. select 单据号 from xx group by 单据号 having count(1)>0，校验是否有1个单据号对应多条不同结果的记录，有则打印
    4. 过滤掉状态为未审核的记录，并打印出来
    '''
    try:
        # ====== [第一步] 客户名称处理 ======
        # 1. 获取客户名称包含葫芦娃的列，series
        filter_1 = data_frame['客户名称'].str.contains(r".*葫芦娃.*")
        # 2. 新增一列tmp, 如果客户名称包含葫芦娃则为1
        data_frame["tmp"] = np.where(filter_1, 1, 0)

        # 3. 按单据号分组，取tmp的sum值，dataframe
        aggr_sum = data_frame.groupby("单据号").agg({"tmp": sum})
        # 4. 过滤出sum>=1的单据号，series
        filter_2 = aggr_sum[aggr_sum["tmp"] >= 1]
        # 5. 上述series取index, 如果单据号在这个index list里，则客户名称改为葫芦娃，否则改为其他
        data_frame["客户名称"] = np.where(data_frame["单据号"].isin(filter_2.index), "葫芦娃", "其他")
        # 6. 去掉tmp列
        data_frame = data_frame.drop('tmp', axis=1)

        # ====== [第二步] 整表去重 ======
        data_frame = data_frame.drop_duplicates()
        # ====== [第三步] 检查是否有异常记录 ======
        key_cnt = data_frame.groupby("单据号").size()  # group by 单据号
        if len(key_cnt[key_cnt > 1]):  # having count(1)>1
            tmp = key_cnt[key_cnt > 1]  # 返回的是Series列表，直接打印的话最底下有一行name/dtype影响阅读
            tmp_res = pd.DataFrame({'单据号': tmp.index, '派送次数': tmp.values})  # 因此转成dataframe, 给size列增加名字
            print("Excel内容异常：如下单据号可能存在多条派送记录，请检查！\n%s" % tmp_res.to_string(index=False))  # 打印时去掉最左侧的默认索引0123
        # ====== [第四步] 过滤状态为未审核的记录 ======
        filter_3 = data_frame['状态'] != "已审核"
        if len(data_frame[filter_3]):
            tmp_res = data_frame[filter_3][["单据号", "状态"]]
            print("Excel内容异常：如下单据号非【已审核】，补贴金额统计为0！\n%s" % tmp_res.to_string(index=False))  # 打印时去掉最左侧的默认索引0123
        data_frame = data_frame[data_frame['状态'] == "已审核"]
    except:
        print("Something error: ", traceback.format_exc())
    finally:
        return data_frame


def main(rule_file, data_file, result_file):
    # 默认读第一个sheet, header=3代表从第4行开始读, 只读A-J列，4-76行; 为空时指定字段用""填充，其他字段为空用0填充；最后转成key:value list
    rule_list = pd.read_excel(rule_file, header=3, usecols="A:J", nrows=72,
                              dtype={"车牌号": str, "客户名称": str, "驾驶员2": str, "送书重量": str, "回头车拉货": str}
                              ).fillna({"车牌号": "", "客户名称": "", "驾驶员2": "", "送书重量": "", "回头车拉货": ""}) \
        .fillna(0) \
        .to_dict(orient="records")

    drive_bill_raw = pd.read_excel(data_file,
                                   usecols=["状态", "单据号", "车牌号", "客户名称", "驾驶员2", "送书重量", "回头车拉货", "驾驶员",
                                            "跟车员1", "跟车员2", "跟车员3", "跟车员4", "跟车员5", "跟车员6"],
                                   dtype={"送书重量": float}
                                   ).dropna(subset=["车牌号"]).fillna({"送书重量": 0}).fillna("")

    drive_bill = data_filter_deduplicate(data_frame=drive_bill_raw).to_dict(orient="records")

    final_result_list = []
    for data in drive_bill:
        fail_num = 0
        for rule in rule_list:
            # print(fail_num, check_client_name(rule["客户名称"], data["客户名称"]),check_back_car(rule["回头车拉货"], data["回头车拉货"]),check_weight(rule["送书重量"], data["送书重量"]),check_driver2(rule["驾驶员2"], data["驾驶员2"]))
            if rule["车牌号"] in data["车牌号"] \
                    and check_client_name(rule["客户名称"], data["客户名称"]) \
                    and check_back_car(rule["回头车拉货"], data["回头车拉货"]) \
                    and check_weight(rule["送书重量"], data["送书重量"]) \
                    and check_driver2(rule["驾驶员2"], data["驾驶员2"]):

                total_driver_amount = rule["车牌补贴"] + rule["葫芦娃补贴"] + rule["回头车补贴"] + float(rule["重量(单价/吨)"]) * float(
                    data["送书重量"])
                total_driver2_amount = rule["驾驶员2补贴"]

                try:
                    single_record_result = amount_allocate(bill_no=data["单据号"],
                                                           driver_amount=total_driver_amount,
                                                           driver2_amount=total_driver2_amount,
                                                           driver=data["驾驶员"],
                                                           driver2=data["驾驶员2"],
                                                           assis1=data["跟车员1"],
                                                           assis2=data["跟车员2"],
                                                           assis3=data["跟车员3"],
                                                           assis4=data["跟车员4"],
                                                           assis5=data["跟车员5"],
                                                           assis6=data["跟车员6"]
                                                           )
                except Exception as e:
                    print(e)
                else:
                    final_result_list += single_record_result
                finally:
                    break
            else:
                fail_num += 1
            if fail_num >= 72:
                # if 6<=fail_num <= 7:
                print("-----fail, scenario does not exist----", fail_num)
                print(data)
                print("-----fail----")

    final_result = pd.DataFrame(final_result_list)
    grouped = final_result.groupby("姓名").agg({"补贴金额": "sum"})

    with pd.ExcelWriter(result_file) as writer:
        final_result.to_excel(writer, sheet_name='补贴明细', index=False)
        grouped.to_excel(writer, sheet_name='金额合计')


if __name__ == "__main__":
    now = time.strftime('%Y%m%d_%H_%M_%S', time.localtime(int(time.time())))

    rule_file = dir + r'/规则场景_可改绿色格子内容.xlsx'
    data_file = dir + r'/派车单明细.xlsx'
    result_file = dir + r'/统计结果_%s.xlsx' % now

    main(rule_file, data_file, result_file)
    print("\n计算完成，结果见文件【%s】\n弹窗1分钟后自动关闭，也可手动关闭~" % result_file)
    time.sleep(60)
