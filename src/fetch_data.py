import sys

import tushare as ts
import pandas as pd
import os
import time

TOKEN = os.environ["TUSHARE_TOKEN"]
ts.set_token(TOKEN)
pro = ts.pro_api()

STOCKS = [
    # 银行
    '600036.SH','600016.SH','601328.SH','000001.SZ','601318.SH',
    '601166.SH','600000.SH','601288.SH','601398.SH','601939.SH',
    # 白酒
    '600519.SH','000858.SZ','000596.SZ','600600.SH','002304.SZ',
    '000799.SZ','603198.SH','600779.SH',
    # 新能源/科技
    '300750.SZ','002594.SZ','000725.SZ','002415.SZ','601012.SH',
    '688599.SH','002241.SZ','300274.SZ','600741.SH','601127.SH',
    # 医药
    '600887.SH','000538.SZ','002007.SZ','300015.SZ','600276.SH',
    '002673.SZ','300347.SZ','603259.SH','600196.SH','000999.SZ',
    # 地产
    '600048.SH','000002.SZ','001979.SZ','600383.SH','000069.SZ',
    '600606.SH',
    # 消费/零售
    '600009.SH','600690.SH','000651.SZ','000333.SZ','601888.SH',
    '600测试28.SH','002032.SZ','600测试585.SH',
    # 钢铁/化工
    '600019.SH','000709.SZ','600188.SH','000783.SZ','600352.SH',
    # 传媒/互联网
    '300059.SZ','002415.SZ','603444.SH',
    # 交通运输
    '600111.SH','601111.SH','600115.SH','601600.SH',
]

# 去掉写错的股票代码
STOCKS = [s for s in STOCKS if '测试' not in s]
STOCKS = list(dict.fromkeys(STOCKS))  # 去重

PERIODS = ['20211231', '20221231', '20231231']

def fetch_financial_data():
    stock_info = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
    name_map = dict(zip(stock_info['ts_code'], stock_info['name']))
    industry_map = dict(zip(stock_info['ts_code'], stock_info['industry']))

    rows = []
    for ts_code in STOCKS:
        for period in PERIODS:
            try:
                fi = pro.fina_indicator(
                    ts_code=ts_code, period=period,
                    fields='ts_code,end_date,debt_to_assets,netprofit_margin,roe,roa,grossprofit_margin'
                )
                if fi.empty:
                    continue
                r = fi.iloc[0]
                def pct(v):
                    return round(float(v) / 100, 6) if v is not None and str(v) != 'nan' else None
                rows.append({
                    'StockCode': ts_code,
                    'CompanyName': name_map.get(ts_code, ''),
                    'Industry': industry_map.get(ts_code, ''),
                    'Period': r['end_date'],
                    'DebtAssetRatio': pct(r['debt_to_assets']),
                    'NetProfitMargin': pct(r['netprofit_margin']),
                    'ROE': pct(r['roe']),
                    'ROA': pct(r['roa']),
                    'GrossProfitMargin': pct(r['grossprofit_margin']),
                })
                time.sleep(0.12)  # 避免触发频率限制
            except Exception:
                continue

    if not rows:
        print("数据获取失败，使用备用模拟数据")
        _generate_mock_data()
        return

    df = pd.DataFrame(rows).dropna()
    df = df[df['CompanyName'].str.strip() != '']
    os.makedirs('data', exist_ok=True)
    df.to_csv('data/financial_data.csv', index=False, encoding='utf-8-sig')
    print(f"数据已保存：data/financial_data.csv，共 {len(df)} 条")


def _generate_mock_data():
    import random
    industries = {
        '银行':   ((60, 90),  (20, 40),  (10, 18)),
        '白酒':   ((20, 50),  (30, 55),  (20, 35)),
        '新能源': ((30, 60),  (5,  20),  (8,  20)),
        '医药':   ((25, 55),  (10, 30),  (10, 22)),
        '地产':   ((65, 85),  (3,  12),  (5,  15)),
        '消费':   ((30, 60),  (8,  25),  (10, 20)),
        '化工':   ((40, 70),  (5,  18),  (8,  15)),
    }
    companies = [
        ('600036.SH','招商银行','银行'), ('600016.SH','民生银行','银行'),
        ('601318.SH','中国平安','银行'), ('000001.SZ','平安银行','银行'),
        ('600519.SH','贵州茅台','白酒'), ('000858.SZ','五粮液','白酒'),
        ('300750.SZ','宁德时代','新能源'), ('002594.SZ','比亚迪','新能源'),
        ('600887.SH','伊利股份','医药'), ('000538.SZ','云南白药','医药'),
        ('600048.SH','保利发展','地产'), ('000002.SZ','万科A','地产'),
        ('000651.SZ','格力电器','消费'), ('000333.SZ','美的集团','消费'),
        ('600019.SH','宝山钢铁','化工'), ('600188.SH','兖矿能源','化工'),
    ]
    rows = []
    for code, name, ind in companies:
        dr, nm, roe_r = industries[ind]
        for period in ['20211231', '20221231', '20231231']:
            debt = round(random.uniform(dr[0], dr[1]), 4)
            margin = round(random.uniform(nm[0], nm[1]), 4)
            roe = round(random.uniform(roe_r[0], roe_r[1]), 4)
            roa = round(roe * (1 - debt / 100), 4)
            gross = round(margin + random.uniform(5, 15), 4)
            rows.append({
                'StockCode': code, 'CompanyName': name, 'Industry': ind,
                'Period': period,
                'DebtAssetRatio': debt, 'NetProfitMargin': margin,
                'ROE': roe, 'ROA': roa, 'GrossProfitMargin': gross,
            })
    df = pd.DataFrame(rows)
    os.makedirs('data', exist_ok=True)
    df.to_csv('data/financial_data.csv', index=False, encoding='utf-8-sig')
    print(f"模拟数据已保存：data/financial_data.csv，共 {len(df)} 条")


if __name__ == '__main__':
    fetch_financial_data()
