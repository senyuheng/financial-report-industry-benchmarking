import pandas as pd
import numpy as np
from faker import Faker
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

fake = Faker('zh_CN')
np.random.seed(42)

# 从真实数据学习行业分布
real = pd.read_csv(os.path.join(BASE_DIR, 'data', 'financial_data.csv'), encoding='utf-8-sig')

# 计算每个行业的统计参数 + 裁剪异常值
stats_df = real.groupby('Industry').agg(
    debt_mean=('DebtAssetRatio', 'mean'),
    debt_std=('DebtAssetRatio', 'std'),
    margin_mean=('NetProfitMargin', 'mean'),
    margin_std=('NetProfitMargin', 'std'),
    roe_mean=('ROE', 'mean'),
    roe_std=('ROE', 'std'),
).fillna(0.05)
# 裁掉异常值：净利率限制在 -50%~60%
stats_df['margin_mean'] = stats_df['margin_mean'].clip(-0.5, 0.6)
industry_stats = stats_df.to_dict('index')

print("学习到的行业分布：")
for ind, stats in industry_stats.items():
    print(f"  {ind}: 负债率均值={stats['debt_mean']:.2f}, 净利率均值={stats['margin_mean']:.2f}")

# 行业营收基数（亿元），基于常识设定
REVENUE_BASE = {
    '银行': (8000e8, 2000e8), '白酒': (500e8, 200e8),
    '新能源': (1500e8, 500e8), '医药': (300e8, 100e8),
    '地产': (2000e8, 800e8), '啤酒': (400e8, 100e8),
    '证券': (600e8, 200e8), '汽车': (1200e8, 400e8),
}

INDUSTRIES = list(industry_stats.keys())
YEARS = [2019, 2020, 2021, 2022, 2023]
QUARTERS = [1, 2, 3, 4]

companies = [(str(i).zfill(6), fake.company(), np.random.choice(INDUSTRIES)) for i in range(50000)]

rows = []
for cid, cname, ind in companies:
    stats = industry_stats.get(ind, industry_stats[INDUSTRIES[0]])
    base, std = REVENUE_BASE.get(ind, (500e8, 200e8))

    for year in YEARS:
        growth = 1 + (year - 2019) * np.random.uniform(0.02, 0.10)
        for quarter in QUARTERS:
            revenue = abs(np.random.normal(base, std)) * growth / 4
            debt = np.clip(np.random.normal(stats['debt_mean'], max(stats['debt_std'], 0.05)), 0.1, 0.95)
            margin = np.random.normal(stats['margin_mean'], max(stats['margin_std'], 0.02))
            profit = revenue * margin
            assets = revenue / max(1 - debt, 0.1) * np.random.uniform(0.8, 1.2)
            liabilities = assets * debt
            cash_flow = profit * np.random.uniform(0.6, 1.4)
            rows.append([
                cid, cname, ind, year, quarter,
                round(revenue, 2), round(profit, 2), round(assets, 2),
                round(liabilities, 2), round(cash_flow, 2)
            ])

df = pd.DataFrame(rows, columns=[
    'company_id','company_name','industry','year','quarter',
    'revenue','profit','assets','liabilities','cash_flow'
])
os.makedirs('data', exist_ok=True)
df.to_parquet('data/finance_raw.parquet', index=False)

# 按行业/年度分区存储
for ind in df['industry'].unique():
    for yr in df['year'].unique():
        sub = df[(df['industry']==ind) & (df['year']==yr)]
        path = f"data/partitioned/industry={ind}/year={yr}"
        os.makedirs(path, exist_ok=True)
        sub.to_parquet(f"{path}/data.parquet", index=False)

print(f"生成完成：{len(df):,} 条，{df['industry'].nunique()} 行业 × {df['year'].nunique()} 年度")
