import os
from flask import Flask, render_template
import pymysql
import json

app = Flask(__name__)

DB = dict(host='localhost', port=3306, user='root', password=os.environ.get("MYSQL_PASSWORD", ""),
          database='finance_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def query(sql, args=None):
    conn = pymysql.connect(**DB)
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall()


@app.route('/')
def index():
    # 风险分布
    risk = query("SELECT RiskLevel, COUNT(*) as cnt FROM finance_analysis GROUP BY RiskLevel")
    # 净利率 TOP10
    top10 = query("SELECT CompanyName, AVG(NetProfitMargin) as margin FROM finance_analysis GROUP BY CompanyName ORDER BY margin DESC LIMIT 10")
    # 资产负债率最高10家
    debt_top = query("SELECT CompanyName, AVG(DebtAssetRatio) as ratio FROM finance_analysis GROUP BY CompanyName ORDER BY ratio DESC LIMIT 10")
    # 总数统计
    stats = query("SELECT COUNT(*) as total, COUNT(DISTINCT StockCode) as companies, COUNT(DISTINCT Period) as periods FROM finance_analysis")[0]

    return render_template('index.html',
        risk_data=json.dumps([r['RiskLevel'] for r in risk], ensure_ascii=False),
        risk_cnt=json.dumps([r['cnt'] for r in risk]),
        top10_names=json.dumps([r['CompanyName'] for r in top10], ensure_ascii=False),
        top10_margins=json.dumps([round(float(r['margin']), 4) for r in top10]),
        debt_names=json.dumps([r['CompanyName'] for r in debt_top], ensure_ascii=False),
        debt_ratios=json.dumps([round(float(r['ratio']), 4) for r in debt_top]),
        stats=stats
    )


@app.route('/table')
def table():
    rows = query("""
        SELECT StockCode, CompanyName, Industry, Period,
               ROUND(NetProfitMargin*100, 2) as 净利率,
               ROUND(GrossProfitMargin*100, 2) as 毛利率,
               ROUND(DebtAssetRatio*100, 2) as 资产负债率,
               ROUND(ROE*100, 2) as ROE,
               ROUND(ROA*100, 2) as ROA,
               RiskLevel
        FROM finance_analysis ORDER BY DebtAssetRatio DESC LIMIT 200
    """)
    return render_template('table.html', rows=rows)


if __name__ == '__main__':
    app.run(debug=True, port=5000)
