import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import warnings
# 忽略读取警告
warnings.filterwarnings('ignore', category=UserWarning, message="Workbook contains no default style")

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

def find_file_and_load(dict,folder_path,filename_str):
    '''
    根据参数字典到对应文件夹获取带有关键词的文件，读取为df并存储在字典中
    '''
    result_dict = {}
    for key, d in dict.items():
        filename = filename_str + f"{d.strftime('%Y%m%d')}"
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
                    result_dict[key] = df
                    found = True
                    break
                except Exception as e:
                    raise ValueError(f"读取文件 {full_path} 失败: {e}")
        if not found:
            raise FileNotFoundError(f"未找到文件: {filename}[.csv|.xlsx]")
    return result_dict

def load_data(date_str):
    """
    根据传入的日期读取相关文件并返回字典
    
    :param folder_path: 文件夹路径
    :param date_str: 8位日期字符串，格式为YYYYMMDD
    :return: 包含DataFrame的字典
    """
    print("开始读取加工目标表所需的源文件...\n")

    date_obj = datetime.strptime(date_str, "%Y%m%d").date()
    folder_path = os.path.join(abs_path,'参考文件')

    if get_last_workday(date_obj).month != date_obj.month:
        cross_month = 2
    elif get_last_wednesday(date_obj).month != date_obj.month:
        cross_month = 1
    else:
        cross_month = 0

    # 读取理财经理详细信息
    manager_dates = {
        'manager_today': date_obj,
        'manager_lm': last_day_of_last_month(date_obj)
    }
    
    manager_dict = find_file_and_load(manager_dates,folder_path,'理财经理详细信息')

    # 读取投资理财销售量统计表
    # sales_dates = {
    #     'sales_today': date_obj,
    #     'sales_lm': last_day_of_last_month(date_obj),
    #     'sales_ld': get_last_workday(date_obj),
    #     'sales_lw': get_last_wednesday(date_obj)
    # }
    #
    # sales_dict = find_file_and_load(sales_dates,folder_path,'投资理财销售量统计表')

    # 读取投资理财中收统计表
    interbusi_dates = {
        'interbusi_today': date_obj,
        'interbusi_lm': last_day_of_last_month(date_obj),
        'interbusi_lw': get_last_wednesday(date_obj)
    }
    
    interbusi_dict = find_file_and_load(interbusi_dates,folder_path,'投资理财中收统计表')

    print("加工目标表所需的源文件已读取完毕。\n")
    # 合并字典
    return {**manager_dict,**interbusi_dict},cross_month
    # return {**manager_dict,**sales_dict,**interbusi_dict},cross_month

def get_prov_match(file_path):
    '''读取分行全简称对应关系及组别分类'''
    return pd.read_excel(os.path.join(file_path))

# 处理柜员信息表并与对应关系关联
def process_manager(df_manager,df_match):
    df_manager_match = pd.merge(df_manager,df_match,on = '总行/一级分行名称',how = 'left')
    column_need = ["序号","柜员号","姓名","总行/一级分行名称","分行","组别"]
    return df_manager_match[column_need]

# 处理销售明细情况与分行情况关联获得分行及组别
def process_sales(df_sales,df_manager_match):
    df_sales_processed = pd.merge(df_manager_match,df_sales,how = 'left',left_on = '柜员号',right_on = '人员工号',suffixes=('_manager', '_sales'))
    df_sales_processed['理财/资管'] = df_sales_processed['理财']+df_sales_processed['资产管理计划']
    df_sales_processed['贵金属'] = df_sales_processed['实物贵金属'] + df_sales_processed['黄金积存']
    column_need = ["柜员号","姓名","总行/一级分行名称","分行","组别","理财/资管","理财","保险","基金","资产管理计划","贵金属","实物贵金属","黄金积存","合计"]
    return df_sales_processed[column_need]

# 处理中收统计情况与分行情况关联获得分行及组别
def process_interbusi(df_interbusi,df_manager_match):
    df_interbusi_processed = pd.merge(df_manager_match,df_interbusi,how = 'left',left_on = '柜员号',right_on = '人员工号',suffixes=('_manager', '_interbusi'))
    df_interbusi_processed['理财/资管'] = df_interbusi_processed['理财']+df_interbusi_processed['资产管理计划']
    df_interbusi_processed['贵金属'] = df_interbusi_processed['实物贵金属'] + df_interbusi_processed['黄金积存']
    df_interbusi_processed['4类业务合计'] = df_interbusi_processed['理财/资管'] + df_interbusi_processed['保险'] + df_interbusi_processed['基金'] + df_interbusi_processed['贵金属']
    column_need = ["柜员号","姓名","总行/一级分行名称","分行","组别","4类业务合计","理财/资管","理财","保险","基金","资产管理计划","贵金属","实物贵金属","黄金积存"]
    return df_interbusi_processed[column_need]

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
    for i in dfs.keys():
        if 'interbusi' in i:
            dfs_processed[i] = process_interbusi(dfs[i],dfs_processed['manager_today']).fillna(0)
        else: continue

    return dfs_processed

# 加工理财经理开单情况
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
        return merge_df2,merge_result

    elif cross_month == 1:
        merge_df3 = pd.merge(merge_df2,sales_last_month,on = '柜员号',how = 'left',suffixes=('', '_lm'))

        for col in asset_columns:
            merge_result[f"{col}_本日"] = merge_df3[col]-merge_df3[f"{col}_yd"]
            merge_result[f"{col}_本周累积"] = merge_df3[col]-merge_df3[f"{col}_lw"] + merge_df3[f"{col}_lm"]

        merge_result['本日开单业务数'] = (merge_result[['理财/资管_本日','保险_本日','基金_本日','贵金属_本日']] > 0).sum(axis=1)
        merge_result['本周累积开单业务数'] = (merge_result[['理财/资管_本周累积','保险_本周累积','基金_本周累积','贵金属_本周累积']] > 0).sum(axis=1)

        return merge_df3,merge_result

    elif cross_month == 2:
        merge_df3 = pd.merge(merge_df2,sales_last_month,on = '柜员号',how = 'left',suffixes=('', '_lm'))

        for col in asset_columns:
            merge_result[f"{col}_本日"] = merge_df3[col]-merge_df3[f"{col}_yd"] + merge_df3[f"{col}_lm"]
            merge_result[f"{col}_本周累积"] = merge_df3[col]-merge_df3[f"{col}_lw"] + merge_df3[f"{col}_lm"]

        merge_result['本日开单业务数'] = (merge_result[['理财/资管_本日','保险_本日','基金_本日','贵金属_本日']] > 0).sum(axis=1)
        merge_result['本周累积开单业务数'] = (merge_result[['理财/资管_本周累积','保险_本周累积','基金_本周累积','贵金属_本周累积']] > 0).sum(axis=1)

        return merge_df3,merge_result
    
# 加工理财中收情况
def manager_interbusi_situ(interbusi_today,interbusi_last_week,interbusi_last_month,cross_month):

    merge_df = pd.merge(interbusi_today,interbusi_last_week,on = '柜员号',how = 'left',suffixes=('', '_lw'))

    merge_result = merge_df[["柜员号","姓名","分行"]].copy()
    # 需要排名的资产列列表
    asset_columns = ['4类业务合计', '理财/资管', '理财', '保险', '基金',
                    '资产管理计划', '贵金属', '实物贵金属', '黄金积存']

    if cross_month == 0:
        for col in asset_columns:
            merge_result[f"{col}_本周"] = merge_df[col]-merge_df[f"{col}_lw"]
        sort_fund = merge_result.sort_values(by='4类业务合计_本周', ascending=False)
        sort_fund['4类业务合计_本周'] = round(sort_fund['4类业务合计_本周'] / 10000)
        sort_fund['4类业务合计_本周'] = sort_fund.rename(columns={'4类业务合计_本周': '4类业务合计（万元）'}, inplace=True)
        sort_fund = sort_fund[['分行', '姓名', '4类业务合计（万元）']]
    elif cross_month == 2:
        merge_df = pd.merge(merge_df,interbusi_last_month,on = '柜员号',how = 'left',suffixes=('', '_lm'))
        for col in asset_columns:
            merge_result[f"{col}_本周"] = merge_df[col]-merge_df[f"{col}_lw"] + merge_df[f"{col}_lm"]
        sort_fund = merge_result.sort_values(by='4类业务合计_本周', ascending=False)
        sort_fund['4类业务合计_本周'] = round(sort_fund['4类业务合计_本周'] / 10000)
        sort_fund['4类业务合计_本周'] = sort_fund.rename(columns={'4类业务合计_本周': '4类业务合计（万元）'}, inplace=True)
        sort_fund = sort_fund[['分行', '姓名', '4类业务合计（万元）']]
    new_columns_order = ["柜员号","姓名","分行"]
    # 批量生成排名列
    for col in asset_columns:
        rank_col = f'{col}_排名'
        merge_result[rank_col] = merge_result[f"{col}_本周"].rank(ascending=False, method='min').astype(int)
        new_columns_order.extend([f"{col}_本周",rank_col])
    
    return merge_df,merge_result[new_columns_order],sort_fund

def generate_sales_df(df,last_month_df):

    print("开始生成结果报表...\n")
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

    print("最终结果报表已生成完毕。\n")

    return group_persons_result,df_rate_result

def generate_interbusi_df(df):
    print("开始生成结果报表...\n")

    # 预处理：过滤无效数据
    filtered = df[df['分行'] != '总行'].copy()

    # 按分行分组聚合计算各项报表信息
    agg_dict={"总人数":pd.NamedAgg(column='柜员号',aggfunc='nunique'),
              "TOP人数":pd.NamedAgg(column='4类业务合计_排名',aggfunc=lambda x: (x <= 1000).sum()),
              "4类业务合计":pd.NamedAgg(column='4类业务合计_本周',aggfunc='sum'),
              }

    group_interbusi_result = filtered.groupby('分行', as_index=False).agg(**agg_dict)

    # 创建汇总字典，计算全国开单人数
    sum_data = { '分行': '总计'}
    # 对其他数值列求和
    for col in group_interbusi_result.columns:
        if col not in [ '分行']:
            sum_data[col] = group_interbusi_result[col].sum()

    # 将汇总行转换为DataFrame并拼接在末尾
    df_sum = pd.DataFrame([sum_data])
    group_interbusi_result = pd.concat([group_interbusi_result,df_sum]).reset_index(drop=True)

    group_interbusi_result['TOP占比'] = (group_interbusi_result['TOP人数'] / group_interbusi_result['总人数']).round(4)
    group_interbusi_result['人均中收'] = (group_interbusi_result['4类业务合计'] / group_interbusi_result['总人数']).round(2)
    group_interbusi_result['人均中收(万元)'] = (group_interbusi_result['人均中收']/10000).round(2)

    # 将目标表按照人均中收进行排序
    main_data = group_interbusi_result[group_interbusi_result['分行'] != '总计']
    total_row = group_interbusi_result[group_interbusi_result['分行'] == '总计']

    sorted_main = main_data.sort_values(by='人均中收', ascending=False)
    group_interbusi_result = pd.concat([sorted_main, total_row], ignore_index=True)

    columns_order = ['分行','总人数','TOP人数','TOP占比','4类业务合计','人均中收','人均中收(万元)']

    print("最终结果报表已生成完毕。\n")

    return group_interbusi_result[columns_order].rename(columns={'分行':'一级名称'})

def to_excel_change_index(df,writer,sheetname):
    df_export = df.reset_index(drop=True)  # 先重置为默认索引
    df_export.index += 1                   # 索引从1开始
    df_export.index.name = '序号'          # 设置索引列名

    # 导出到 Excel
    df_export.to_excel(writer, sheet_name=sheetname)

def generate_sales_report(param_date):

    print(f"开始生成{param_date}日期的理财经理开单情况统计表...\n")

    dfs,cross_month = load_data(param_date)
    dfs_processed = all_df_reduce(dfs,df_match)

    sales_mid,df_sales_result = manager_sales_situ(dfs_processed['sales_today'],dfs_processed['sales_ld'],dfs_processed['sales_lw'],dfs_processed['sales_lm'],cross_month)
    group_persons_result,df_rate_result = generate_sales_df(df_sales_result,dfs_processed['manager_lm'])

    sales_output_path = os.path.join(abs_path,'理财经理开单情况统计表',f'理财经理开单情况统计表{param_date}.xlsx') 

    print(f"正在生成：理财经理开单情况统计表{param_date}.xlsx...\n")

    with pd.ExcelWriter(sales_output_path, engine='openpyxl') as writer:
        to_excel_change_index(df_rate_result,writer,'结果通报表') 
        to_excel_change_index(sales_mid,writer,'多日明细情况') 
        to_excel_change_index(df_sales_result,writer,'用户中间表') 
        to_excel_change_index(group_persons_result,writer,'机构中间表') 

    print(f"理财经理开单情况统计表已保存至：{sales_output_path}\n")

def generate_interbusi_report(param_date):

    print(f"开始生成{param_date}日期的投资理财中收统计表...\n")

    dfs,cross_month = load_data(param_date)
    dfs_processed = all_df_reduce(dfs,df_match)

    interbusi_mid,df_interbusi_result,business_sort = manager_interbusi_situ(dfs_processed['interbusi_today'],dfs_processed['interbusi_lw'],dfs_processed['interbusi_lm'],cross_month)
    group_interbusi_result = generate_interbusi_df(df_interbusi_result)

    if not os.path.exists('./业务TOP周榜单'):
        os.makedirs('./业务TOP周榜单')
    # 写入文件业务TOP周榜单
    print(f"正在生成：中间业务收入TOP榜单{param_date}.xlsx...\n")
    output_path = os.path.join(abs_path, '业务TOP周榜单', f'中间业务收入TOP榜单{param_date}.xlsx')
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        business_sort.to_excel(writer, sheet_name='中间业务收入TOP榜单', index=False)
    print(f"投资理财中收统计表已保存至：{output_path}\n")

    if not os.path.exists('./投资理财中收统计表'):
        os.makedirs('./投资理财中收统计表')
    interbusi_output_path = os.path.join(abs_path,'投资理财中收统计表',f'投资理财中收统计表{param_date}.xlsx') 

    print(f"正在生成：投资理财中收统计表{param_date}.xlsx...\n")

    with pd.ExcelWriter(interbusi_output_path, engine='openpyxl') as writer:
        to_excel_change_index(group_interbusi_result,writer,'结果通报表') 
        to_excel_change_index(interbusi_mid,writer,'多日明细情况') 
        to_excel_change_index(df_interbusi_result,writer,'用户中间表') 

    print(f"投资理财中收统计表已保存至：{interbusi_output_path}\n")

if __name__ == "__main__":

    # 检查是否有足够的参数
    if len(sys.argv) != 2:
        print("用法: python generate_report.py <YYYYMMDD>")
        sys.exit(1)  # 如果没有参数，退出程序

    param_date = sys.argv[1]
    abs_path = os.getcwd()
    df_match = get_prov_match(os.path.join(abs_path,'分行全简称对应及组别分类.xlsx'))

    # generate_sales_report(param_date)
    generate_interbusi_report(param_date)

