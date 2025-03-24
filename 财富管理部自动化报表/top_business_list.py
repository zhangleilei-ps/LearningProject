import os
import sys
import warnings

import pandas as pd
from datetime import datetime, timedelta

# 忽略读取警告
warnings.filterwarnings('ignore', category=UserWarning, message="Workbook contains no default style")
# 检查是否有足够的参数
if len(sys.argv) != 2:
    print("用法: python 111.py <YYYYMMDD>")
    sys.exit(1)  # 如果没有参数，退出程序

param_date = sys.argv[1]
abs_path = os.getcwd()

def get_last_workday(date_obj):
    """计算上一个工作日（周一返回上周五，周末返回周五，其他返回前一日）"""
    if date_obj.isoweekday() == 1:  # 周一
        return date_obj - timedelta(days=3)
    elif date_obj.isoweekday() >= 6:  # 周六或周日
        return date_obj - timedelta(days=(date_obj.isoweekday() - 5))
    else:  # 周二到周五
        return date_obj - timedelta(days=1)

def get_last_wednesday(date_obj):
    """计算上一个周三（当天为周三时取七天前）"""
    current_weekday = date_obj.isoweekday()
    # 当天是周三时 delta=7，否则按原逻辑计算
    delta = 7 if current_weekday == 3 else (current_weekday - 3) % 7
    return date_obj - timedelta(days=delta)

def last_day_of_last_month(date_obj):
    """计算传入日期的上个月的最后一天"""
    first_day_of_current_month = date_obj.replace(day=1)
    return first_day_of_current_month - timedelta(days=1)


def load_data(date_str):
    """
    根据传入的日期读取相关文件并返回字典

    :param folder_path: 文件夹路径
    :param date_str: 8位日期字符串，格式为YYYYMMDD
    :return: 包含DataFrame的字典
    """
    date_obj = datetime.strptime(date_str, "%Y%m%d").date()
    folder_path = os.path.join(abs_path, '参考文件')
    print(f'正在读取投资理财销售量统计表{date_str}.xlsx...\n')
    # 处理投资理财销售量统计表
    sales_dates = {
        'sales_today': date_obj,
        'sales_lm': last_day_of_last_month(date_obj),
        'sales_lw': get_last_wednesday(date_obj)
    }

    if get_last_wednesday(date_obj).month != date_obj.month:
        cross_month = 1
    else:
        cross_month = 0

    sales_dict = {}

    for key, d in sales_dates.items():
        filename = f"投资理财销售量统计表{d.strftime('%Y%m%d')}"
        filepath = os.path.join(folder_path, filename)
        found = False
        # 尝试不同的文件扩展名
        for ext in ['.xls', '.xlsx', '.csv']:
            full_path = filepath + ext
            if os.path.exists(full_path):
                try:
                    if ext == '.csv':
                        df = pd.read_csv(full_path)
                    else:
                        df = pd.read_excel(full_path, engine='openpyxl', header=2)
                    sales_dict[key] = df
                    found = True
                    break
                except Exception as e:
                    raise ValueError(f"读取文件 {full_path} 失败: {e}")
        if not found:
            raise FileNotFoundError(f"未找到文件: {filename}[.csv|.xlsx|.xls]")
    print(f'正在读取理财经理详细信息表{date_str}.xlsx...\n')
    # 处理理财经理详细信息
    manager_dates = {
        'manager_today': date_obj
    }

    manager_dict = {}
    for key, d in manager_dates.items():
        filename = f"理财经理详细信息{d.strftime('%Y%m%d')}"
        filepath = os.path.join(folder_path, filename)
        found = False
        for ext in ['.csv', '.xlsx', '.xls']:
            full_path = filepath + ext
            if os.path.exists(full_path):
                try:
                    if ext == '.csv':
                        df = pd.read_csv(full_path)
                    else:
                        df = pd.read_excel(full_path, engine='openpyxl', header=2)
                    manager_dict[key] = df
                    found = True
                    break
                except Exception as e:
                    raise ValueError(f"读取文件 {full_path} 失败: {e}")
        if not found:
            raise FileNotFoundError(f"未找到文件: {filename}[.csv|.xlsx]")

    # 合并字典
    return {**sales_dict, **manager_dict}, cross_month

dfs, cross_month = load_data(param_date)

#加载权益类基金数据
def fund_lodaData(date_str):
    date_obj = datetime.strptime(date_str, "%Y%m%d").date()
    folder_path = os.path.join(abs_path, '参考文件')
    if get_last_wednesday(date_obj).month != date_obj.month:
        sales_dates = {
            'sales_today': date_obj,
            'sales_lm': last_day_of_last_month(date_obj),
            'sales_lw': get_last_wednesday(date_obj)
        }
    else:
        sales_dates = {
            'sales_today': date_obj,
            'sales_lw': get_last_wednesday(date_obj)
        }
    # 处理投资理财销售量统计表
    print(f'正在读取投资理财销售量统计表-权益{date_str}.xlsx...\n')
    fund_dict = {}
    for key, d in sales_dates.items():
        filename = f"投资理财销售量统计表-权益{d.strftime('%Y%m%d')}"
        filepath = os.path.join(folder_path, filename)
        found = False
        for ext in ['.csv', '.xlsx', '.xls']:
            full_path = filepath + ext
            if os.path.exists(full_path):
                try:
                    if ext == '.csv':
                        df = pd.read_csv(full_path)
                    else:
                        df = pd.read_excel(full_path, engine='openpyxl', header=2)
                    fund_dict[key] = df
                    found = True
                    break
                except Exception as e:
                    raise ValueError(f"读取文件 {full_path} 失败: {e}")
        if not found:
            raise FileNotFoundError(f"未找到文件: {filename}[.csv|.xlsx]")
    # 处理理财经理详细信息
    manager_dates = {
        'manager_today': date_obj
    }

    manager_dict = {}
    for key, d in manager_dates.items():
        filename = f"理财经理详细信息{d.strftime('%Y%m%d')}"
        filepath = os.path.join(folder_path, filename)
        found = False
        for ext in ['.csv', '.xlsx', '.xls']:
            full_path = filepath + ext
            if os.path.exists(full_path):
                try:
                    if ext == '.csv':
                        df = pd.read_csv(full_path)
                    else:
                        df = pd.read_excel(full_path, engine='openpyxl', header=2)
                    manager_dict[key] = df
                    found = True
                    break
                except Exception as e:
                    raise ValueError(f"读取文件 {full_path} 失败: {e}")
        if not found:
            raise FileNotFoundError(f"未找到文件: {filename}[.csv|.xlsx]")

    return {**fund_dict, **manager_dict}

fund_dfs = fund_lodaData(param_date)

# 明细销售情况预处理

# 读取分行全简称对应关系及组别分类
df_match = pd.read_excel(os.path.join(abs_path,'分行全简称对应及组别分类.xlsx'))

# 处理柜员信息表并与对应关系关联
def process_manager(df_manager,df_match):
    df_manager_match = pd.merge(df_manager,df_match,on = '总行/一级分行名称',how = 'left')
    column_need = ["序号","柜员号","姓名","总行/一级分行名称","分行"]
    return df_manager_match[column_need]

# 将单日明细情况与分行情况关联获得分行及组别
def process_sales(df_sales,df_manager_match):
    df_sales_processed = pd.merge(df_manager_match,df_sales,how = 'left',left_on = '柜员号',right_on = '人员工号',suffixes=('_manager', '_sales'))
    df_sales_processed['理财/资管'] = df_sales_processed['理财']+df_sales_processed['资产管理计划']
    df_sales_processed['贵金属'] = df_sales_processed['实物贵金属'] + df_sales_processed['黄金积存']
    column_need = ["柜员号","姓名","总行/一级分行名称","分行","理财/资管","理财","保险","基金","资产管理计划","贵金属","实物贵金属","黄金积存","合计"]
    return df_sales_processed[column_need]

# 对df进行预处理
def all_df_reduce(dfs,df_match):

    dfs_processed = {}

    for i in dfs.keys():
        if 'manager' in i :
            dfs_processed[i] = process_manager(dfs[i],df_match).fillna({ '组别': '无组别'})
        else: continue
    for i in dfs.keys():
        if 'sales' in i:
            dfs_processed[i] = process_sales(dfs[i],dfs_processed['manager_today']).fillna(0)
        else: continue

    return dfs_processed

dfs_processed = all_df_reduce(dfs,df_match)

#单独处理权益类基金
#权益基金数据预处理
def process_fund_sales(df_sales,df_manager_match):
    df_sales_processed = pd.merge(df_manager_match,df_sales,how = 'left',left_on = '柜员号',right_on = '人员工号',suffixes=('_manager', '_sales'))
    column_need = ["柜员号","姓名","总行/一级分行名称","分行","基金"]
    return df_sales_processed[column_need]

def fund_df_reduce(dfs,df_match):

    dfs_processed = {}

    for i in dfs.keys():
        if 'manager' in i :
            dfs_processed[i] = process_manager(dfs[i],df_match).fillna({ '组别': '无组别'})
        else: continue
    for i in dfs.keys():
        if 'sales' in i:
            dfs_processed[i] = process_fund_sales(dfs[i],dfs_processed['manager_today']).fillna(0)
        else: continue

    return dfs_processed

fund_dfs_process = fund_df_reduce(fund_dfs,df_match)
# 加工4种业务本周累积销售情况
def manager_sales_situ(sales_today,sales_last_week,sales_last_month,cross_month):

    merge_df2 = pd.merge(sales_today,sales_last_week,on = '柜员号',how = 'left',suffixes=('', '_lw'))

    merge_result = merge_df2[["柜员号","姓名","分行"]].copy()
    asset_columns = ['理财/资管', '保险', '基金', '贵金属']

    if cross_month == 0:

        for col in asset_columns:
            merge_result[f"{col}_本周累积"] = merge_df2[col]-merge_df2[f"{col}_lw"]

        sort_LC = merge_result.sort_values(by='理财/资管_本周累积' , ascending = False)
        sort_LC['理财/资管_本周累积'] = round(sort_LC['理财/资管_本周累积']/10000)
        sort_BX = merge_result.sort_values(by='保险_本周累积' , ascending = False)
        sort_BX['保险_本周累积'] = round(sort_BX['保险_本周累积']/10000)
        sort_JJ = merge_result.sort_values(by='基金_本周累积', ascending=False)
        sort_JJ['基金_本周累积'] = round(sort_JJ['基金_本周累积']/10000)
        sort_GJS = merge_result.sort_values(by='贵金属_本周累积', ascending=False)
        sort_GJS['贵金属_本周累积'] = round(sort_GJS['贵金属_本周累积']/10000)
        sort_LC['理财/资管_本周累积'] = sort_LC.rename(columns={'理财/资管_本周累积':'理财/资管本周销量（万元）'},inplace=True)
        sort_BX['保险_本周累积'] = sort_BX.rename(columns={'保险_本周累积':'保险_本周销量（万元）'},inplace=True)
        sort_JJ['基金_本周累积'] = sort_JJ.rename(columns={'基金_本周累积':'基金_本周销量（万元）'},inplace=True)
        sort_GJS['贵金属_本周累积'] = sort_GJS.rename(columns={'贵金属_本周累积': '贵金属_本周销量（万元）'}, inplace=True)
        sort_LC = sort_LC[['分行', '姓名', '理财/资管本周销量（万元）']]
        sort_BX = sort_BX[['分行', '姓名', '保险_本周销量（万元）']]
        sort_JJ = sort_JJ[['分行','姓名','基金_本周销量（万元）']]
        sort_GJS = sort_GJS[['分行', '姓名', '贵金属_本周销量（万元）']]
        return merge_df2,merge_result,sort_LC,sort_BX,sort_JJ,sort_GJS

    elif cross_month == 1:
        merge_df3 = pd.merge(merge_df2,sales_last_month,on = '柜员号',how = 'left',suffixes=('', '_lm'))

        for col in asset_columns:
            merge_result[f"{col}_本周累积"] = merge_df3[col] + merge_df3[f"{col}_lm"] - merge_df3[f"{col}_lw"]
        sort_LC = merge_result.sort_values(by='理财/资管_本周累积', ascending=False)
        sort_LC['理财/资管_本周累积'] = round(sort_LC['理财/资管_本周累积']/10000)
        sort_BX = merge_result.sort_values(by='保险_本周累积', ascending=False)
        sort_BX['保险_本周累积'] = round(sort_BX['保险_本周累积']/10000)
        sort_JJ = merge_result.sort_values(by='基金_本周累积', ascending=False)
        sort_JJ['基金_本周累积'] = round(sort_JJ['基金_本周累积']/10000)
        sort_GJS = merge_result.sort_values(by='贵金属_本周累积', ascending=False)
        sort_GJS['贵金属_本周累积'] = round(sort_GJS['贵金属_本周累积']/10000)
        sort_LC['理财/资管_本周累积'] = sort_LC.rename(columns={'理财/资管_本周累积': '理财/资管本周销量（万元）'}, inplace=True)
        sort_BX['保险_本周累积'] = sort_BX.rename(columns={'保险_本周累积': '保险_本周销量（万元）'}, inplace=True)
        sort_JJ['基金_本周累积'] = sort_JJ.rename(columns={'基金_本周累积': '基金_本周销量（万元）'}, inplace=True)
        sort_GJS['贵金属_本周累积'] = sort_GJS.rename(columns={'贵金属_本周累积': '贵金属_本周销量（万元）'}, inplace=True)
        sort_LC = sort_LC[['分行', '姓名', '理财/资管本周销量（万元）']]
        sort_BX = sort_BX[['分行', '姓名', '保险_本周销量（万元）']]
        sort_JJ = sort_JJ[['分行', '姓名', '基金_本周销量（万元）']]
        sort_GJS = sort_GJS[['分行', '姓名', '贵金属_本周销量（万元）']]
        return merge_df3, merge_result, sort_LC, sort_BX, sort_JJ, sort_GJS
merge_mid,df_result,sort_LC, sort_BX, sort_JJ, sort_GJS = manager_sales_situ(dfs_processed['sales_today'],dfs_processed['sales_lw'],dfs_processed['sales_lm'],cross_month)

#加工权益基金本周累积销售情况
def fund_top_list(sales_today,sales_last_week,cross_month,**args):
    fund_merge_df = pd.merge(sales_today, sales_last_week, on='柜员号', how='left', suffixes=('', '_lw'))
    merge_result = fund_merge_df[["柜员号", "姓名", "分行"]].copy()
    asset_columns = ['基金']
    if cross_month == 0:
        for col in asset_columns:
            merge_result[f"{col}_本周累积"] = fund_merge_df[col]-fund_merge_df[f"{col}_lw"]
        sort_fund = merge_result.sort_values(by='基金_本周累积', ascending=False)
        sort_fund['基金_本周累积'] = round(sort_fund['基金_本周累积']/10000)
        sort_fund['基金_本周累积'] = sort_fund.rename(columns={'基金_本周累积': '基金_本周销量（万元）'}, inplace=True)
        sort_fund = sort_fund[['分行', '姓名', '基金_本周销量（万元）']]
        return merge_result, sort_fund
    if cross_month == 1:
        sales_last_month = args['last_month_sale']
        fund_merge_df2 = pd.merge(fund_merge_df, sales_last_month, on='柜员号', how='left', suffixes=('', '_lm'))
        for col in asset_columns:
            merge_result[f"{col}_本周累积"] = fund_merge_df2[col] + fund_merge_df2[f"{col}_lm"] - fund_merge_df2[f"{col}_lw"]
        sort_fund = merge_result.sort_values(by='基金_本周累积', ascending=False)
        sort_fund['基金_本周累积'] = round(sort_fund['基金_本周累积'] / 10000)
        sort_fund['基金_本周累积'] = sort_fund.rename(columns={'基金_本周累积': '基金_本周销量（万元）'}, inplace=True)
        sort_fund = sort_fund[['分行', '姓名', '基金_本周销量（万元）']]
        return merge_result, sort_fund

if cross_month == 0:
    fund_merge,fund_top = fund_top_list(fund_dfs_process['sales_today'],fund_dfs_process['sales_lw'],cross_month)
elif cross_month ==1:
    fund_merge,fund_top = fund_top_list(fund_dfs_process['sales_today'],fund_dfs_process['sales_lw'],cross_month,last_month_sale = fund_dfs_process['sales_lm'])

if not os.path.exists('./业务TOP周榜单'):
    os.makedirs('./业务TOP周榜单')
#写入文件业务TOP周榜单
print(f"正在生成：4大业务TOP榜单{param_date}.xlsx...\n")
output_path = os.path.join(abs_path,'业务TOP周榜单',f'4种业务TOP周榜单{param_date}.xlsx')
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    sort_LC.to_excel(writer, sheet_name='理财资管top榜单', index=False)
    sort_BX.to_excel(writer, sheet_name='保险top榜单', index=False)
    sort_GJS.to_excel(writer, sheet_name='贵金属top榜单', index=False)
    fund_top.to_excel(writer, sheet_name='基金top榜单', index=False)
print(f"4大业务TOP榜单数据已保存至：{output_path}\n")



