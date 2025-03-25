import os
import sys
import warnings
import pandas as pd
from datetime import datetime, timedelta


# 忽略读取警告
warnings.filterwarnings('ignore', category=UserWarning, message="Workbook contains no default style")
# 读取并校验执行参数

# 检查是否有足够的参数
if len(sys.argv) != 2:
    print("用法: python 111.py <YYYYMMDD>")
    sys.exit(1)  # 如果没有参数，退出程序

param_date = sys.argv[1]
abs_path = os.getcwd()

def get_first_workday_of_month(date_obj):
    """获取某个月的第一个工作日（周一至周五）"""
    first_day = date_obj.replace(day=1)
    # 处理首日为周末的情况
    if first_day.isoweekday() == 6:   # 周六 -> 下周一（+2天）
        return first_day + timedelta(days=2)
    elif first_day.isoweekday() == 7: # 周日 -> 下周一（+1天）
        return first_day + timedelta(days=1)
    else:
        return first_day

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
    folder_path = os.path.join(abs_path,'参考文件')
    print(f'正在读取投资理财销售量统计表{date_str}.xlsx...\n')
    # 处理投资理财销售量统计表
    sales_dates = {
        'sales_today': date_obj,
        'sales_lm': last_day_of_last_month(date_obj),
        'sales_ld': get_last_workday(date_obj),
        'sales_lw': get_last_wednesday(date_obj)
    }

    if get_last_workday(date_obj).month != date_obj.month:
        cross_month = 2
    elif get_last_wednesday(date_obj).month != date_obj.month:
        cross_month = 1
    else:
        cross_month = 0


    sales_dict = {}
    print(f'正在读取投资理财销售量统计表{date_str}.xlsx...\n')
    for key, d in sales_dates.items():
        filename = f"投资理财销售量统计表{d.strftime('%Y%m%d')}"
        filepath = os.path.join(folder_path, filename)
        found = False
        # 尝试不同的文件扩展名
        for ext in ['.xls', '.xlsx','.csv']:
            full_path = filepath + ext
            if os.path.exists(full_path):
                try:
                    if ext == '.csv':
                        df = pd.read_csv(full_path)
                    else:
                        df = pd.read_excel(full_path,engine = 'openpyxl',header=2)
                    sales_dict[key] = df
                    found = True
                    break
                except Exception as e:
                    raise ValueError(f"读取文件 {full_path} 失败: {e}")
        if not found:
            raise FileNotFoundError(f"未找到文件: {filename}[.csv|.xlsx|.xls]")
    
    # 处理理财经理详细信息
    manager_dates = {
        'manager_today': date_obj,
        'manager_lm': last_day_of_last_month(date_obj)
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
                        df = pd.read_excel(full_path,engine = 'openpyxl',header=2)
                    manager_dict[key] = df
                    found = True
                    break
                except Exception as e:
                    raise ValueError(f"读取文件 {full_path} 失败: {e}")
        if not found:
            raise FileNotFoundError(f"未找到文件: {filename}[.csv|.xlsx]")
    
    # 合并字典
    return {**sales_dict,**manager_dict},cross_month

dfs,cross_month = load_data(param_date)

# 明细销售情况预处理

# 读取分行全简称对应关系及组别分类
df_match = pd.read_excel(os.path.join(abs_path,'分行全简称对应及组别分类.xlsx'))

# 处理柜员信息表并与对应关系关联
def process_manager(df_manager,df_match):
    df_manager_match = pd.merge(df_manager,df_match,on = '总行/一级分行名称',how = 'left')
    column_need = ["序号","柜员号","姓名","总行/一级分行名称","分行","组别"]
    return df_manager_match[column_need]

# 将单日明细情况与分行情况关联获得分行及组别
def process_sales(df_sales,df_manager_match):
    df_sales_processed = pd.merge(df_manager_match,df_sales,how = 'left',left_on = '柜员号',right_on = '人员工号',suffixes=('_manager', '_sales'))
    df_sales_processed['理财/资管'] = df_sales_processed['理财']+df_sales_processed['资产管理计划']
    df_sales_processed['贵金属'] = df_sales_processed['实物贵金属'] + df_sales_processed['黄金积存']
    column_need = ["柜员号","姓名","总行/一级分行名称","分行","组别","理财/资管","理财","保险","基金","资产管理计划","贵金属","实物贵金属","黄金积存","合计"]
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


# 加工当日销售情况
def manager_sales_situ(sales_today,sales_yesterday,sales_last_week,sales_last_month,cross_month):

    merge_df1 = pd.merge(sales_today,sales_yesterday,on = '柜员号',how = 'left',suffixes=('', '_yd'))
    merge_df2 = pd.merge(merge_df1,sales_last_week,on = '柜员号',how = 'left',suffixes=('', '_lw'))

    merge_result = merge_df2[["柜员号","姓名","分行","组别"]].copy()
    asset_columns = ['理财/资管', '保险', '基金', '贵金属']

    if cross_month == 0:

        for col in asset_columns:
            merge_result[f"{col}_本日"] = merge_df2[col]-merge_df2[f"{col}_yd"]
            merge_result[f"{col}_本周累积"] = merge_df2[col]-merge_df2[f"{col}_lw"]

        merge_result['本日开单业务数'] = (merge_result[['理财/资管_本日','保险_本日','基金_本日','贵金属_本日']] > 0).sum(axis=1)
        merge_result['本周累积开单业务数'] = (merge_result[['理财/资管_本周累积','保险_本周累积','基金_本周累积','贵金属_本周累积']] > 0).sum(axis=1)
        merge_result['4种业务开单情况'] = 4*(merge_result['本周累积开单业务数'] ==4).astype(int)
        merge_result['3种业务开单情况'] = 3*(merge_result['本周累积开单业务数'] ==3).astype(int)
        merge_result['2种业务开单情况'] = 2*(merge_result['本周累积开单业务数'] ==2).astype(int)
        merge_result['1种业务开单情况'] = (merge_result['本周累积开单业务数'] ==1).astype(int)
        merge_result['本周0产能'] = (merge_result['本周累积开单业务数']>0).astype(int)
        merge_result['当日0产能'] = (merge_result['本日开单业务数']>0).astype(int)
        return merge_df2,merge_result

    elif cross_month == 1:
        merge_df3 = pd.merge(merge_df2,sales_last_month,on = '柜员号',how = 'left',suffixes=('', '_lm'))

        for col in asset_columns:
            merge_result[f"{col}_本日"] = merge_df3[col]-merge_df3[f"{col}_yd"]
            merge_result[f"{col}_本周累积"] = merge_df3[col] + merge_df3[f"{col}_lm"] - merge_df3[f"{col}_lw"]

        merge_result['本日开单业务数'] = (merge_result[['理财/资管_本日','保险_本日','基金_本日','贵金属_本日']] > 0).sum(axis=1)
        merge_result['本周累积开单业务数'] = (merge_result[['理财/资管_本周累积','保险_本周累积','基金_本周累积','贵金属_本周累积']] > 0).sum(axis=1)
        merge_result['4种业务开单情况'] = 4*(merge_result['本周累积开单业务数'] ==4).astype(int)
        merge_result['3种业务开单情况'] = 3*(merge_result['本周累积开单业务数'] ==3).astype(int)
        merge_result['2种业务开单情况'] = 2*(merge_result['本周累积开单业务数'] ==2).astype(int)
        merge_result['1种业务开单情况'] = (merge_result['本周累积开单业务数'] ==1).astype(int)
        merge_result['本周0产能'] = (merge_result['本周累积开单业务数']>0).astype(int)
        merge_result['当日0产能'] = (merge_result['本日开单业务数']>0).astype(int)
        return merge_df3,merge_result

    elif cross_month == 2:
        merge_df3 = pd.merge(merge_df2,sales_last_month,on = '柜员号',how = 'left',suffixes=('', '_lm'))

        for col in asset_columns:
            merge_result[f"{col}_本日"] = merge_df3[col] + merge_df3[f"{col}_lm"] - merge_df3[f"{col}_yd"]
            merge_result[f"{col}_本周累积"] = merge_df3[col] + merge_df3[f"{col}_lm"] - merge_df3[f"{col}_lw"]

        merge_result['本日开单业务数'] = (merge_result[['理财/资管_本日','保险_本日','基金_本日','贵金属_本日']] > 0).sum(axis=1)
        merge_result['本周累积开单业务数'] = (merge_result[['理财/资管_本周累积','保险_本周累积','基金_本周累积','贵金属_本周累积']] > 0).sum(axis=1)
        merge_result['4种业务开单情况'] = 4*(merge_result['本周累积开单业务数'] ==4).astype(int)
        merge_result['3种业务开单情况'] = 3*(merge_result['本周累积开单业务数'] ==3).astype(int)
        merge_result['2种业务开单情况'] = 2*(merge_result['本周累积开单业务数'] ==2).astype(int)
        merge_result['1种业务开单情况'] = (merge_result['本周累积开单业务数'] ==1).astype(int)
        merge_result['本周0产能'] = (merge_result['本周累积开单业务数']>0).astype(int)
        merge_result['当日0产能'] = (merge_result['本日开单业务数']>0).astype(int)
        return merge_df3,merge_result


merge_mid,df_result = manager_sales_situ(dfs_processed['sales_today'],dfs_processed['sales_ld'],dfs_processed['sales_lw'],dfs_processed['sales_lm'],cross_month)


def generate_report(df,last_month_df):
    # 预处理：过滤无效数据
    filtered = df[df['组别'] != '无组别'].copy()
    last_month_df = last_month_df[last_month_df['组别'] != '无组别'].copy()
    last_month_df = last_month_df.groupby('分行',as_index=False).agg(上月底总人数=('柜员号','nunique'))
    
    # 定义资产类别（与图片列顺序一致）
    assets = ['理财/资管','保险', '基金','贵金属']
    
    agg_dict={"理财经理总人数":pd.NamedAgg(column='柜员号',aggfunc='nunique'),
              **{f'{asset}_本日开单人数':pd.NamedAgg(column=f'{asset}_本日',aggfunc=lambda x: (x > 0).sum()) for asset in assets},
              '本日未开单人数(0产能)':pd.NamedAgg(column='本日开单业务数',aggfunc=lambda x: (x <= 0).sum()),
              **{f'{asset}_本周累积开单人数': pd.NamedAgg(column=f'{asset}_本周累积',aggfunc=lambda x: (x > 0).sum()) for asset in assets},
              '本周累积未开单人数(0产能)':pd.NamedAgg(column='本周累积开单业务数',aggfunc=lambda x: (x <= 0).sum())
              }
    # 按分行分组聚合计算开单人数
    group_persons_result = filtered.groupby(['组别','分行'], as_index=False).agg(**agg_dict)

    # 分组统计不同业务数的人数
    asset_num_result = (
        df.groupby(["分行", "组别"])["本周累积开单业务数"]
        .value_counts()
        .unstack(fill_value=0)
        .reindex(columns=[1, 2, 3, 4], fill_value=0)  # 确保包含所有业务数
        .rename(columns=lambda x: f"{x}种业务开单人数")
        .reset_index()
    )
    group_persons_result = group_persons_result.merge(asset_num_result,on = ['分行','组别'],how = 'left')
    group_persons_result = group_persons_result.merge(last_month_df,on = '分行',how = 'left')

    # 创建汇总字典，计算全国开单人数
    sum_data = {'组别': '无组别', '分行': '全国'}
    # 对其他数值列求和
    for col in group_persons_result.columns:
        if col not in ['组别', '分行']:
            sum_data[col] = group_persons_result[col].sum()

    # 将汇总行转换为DataFrame并调整位置
    df_sum = pd.DataFrame([sum_data])
    group_persons_result = pd.concat([df_sum, group_persons_result]).reset_index(drop=True)

    # 计算开单率
    df_rate_result = group_persons_result [['组别','分行','理财经理总人数']].copy()
    df_rate_result['人数变动'] = group_persons_result ['理财经理总人数'] - group_persons_result ['上月底总人数']

    df_day_rate = pd.DataFrame() # group_persons_result [['组别','分行']].copy()
    df_day_rate['合计开单率'] = ((1 - group_persons_result['本日未开单人数(0产能)'] / group_persons_result["理财经理总人数"]) ).round(4)
    df_week_rate = pd.DataFrame() # group_persons_result [['组别','分行']].copy()
    df_week_rate['合计开单率'] = ((1 - group_persons_result['本周累积未开单人数(0产能)'] / group_persons_result["理财经理总人数"]) ).round(4)

    for col in assets:
        df_day_rate[f"{col}_本日开单率"] = (group_persons_result[f"{col}_本日开单人数"] / group_persons_result["理财经理总人数"] ).round(4)
        df_week_rate[f"{col}_本周累积开单率"] = (group_persons_result[f"{col}_本周累积开单人数"] / group_persons_result["理财经理总人数"] ).round(4)
    
    df_day_rate['未开单人数(0产能)'] = group_persons_result['本日未开单人数(0产能)'] 

    for i in [1,2,3,4]:
        df_week_rate[f"{i}种业务开单率"] = (group_persons_result[f"{i}种业务开单人数"] / group_persons_result["理财经理总人数"] ).round(4)
    df_week_rate['未开单人数(0产能)'] = group_persons_result['本周累积未开单人数(0产能)'] 

    df_rate_result = pd.concat({"":df_rate_result,'本日开单率情况':df_day_rate,'本周开单率情况':df_week_rate},axis=1)
    return group_persons_result,df_rate_result


group_persons_result,df_rate_result = generate_report(df_result,dfs_processed['manager_lm'])

if not os.path.exists('./理财经理开单情况统计表'):
    os.makedirs('./理财经理开单情况统计表')
print(f"正在生成：理财经理开单情况统计表{param_date}.xlsx...\n")
# 将中间结果及结果表写入excel
output_path = os.path.join(abs_path,'理财经理开单情况统计表',f'理财经理开单情况统计表{param_date}.xlsx') 

# 核心写入逻辑
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    df_rate_result.to_excel(writer, sheet_name='结果通报表')
    df_result.to_excel(writer, sheet_name='用户中间表', index=False)
    group_persons_result.to_excel(writer, sheet_name='机构中间表', index=False)

print(f"理财经理开单情况统计表已保存至：{output_path}\n")
