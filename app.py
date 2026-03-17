#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
商品期货ETF套利监测系统 (Web版)
"""

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

# ==========================================
# 页面配置
# ==========================================
st.set_page_config(
    page_title="商品期货ETF套利监测系统（BBS手作）",
    page_icon="🐼",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ==========================================
# 品种配置库
# ==========================================
COMMODITY_DB = {
    'AL':  {'name': '沪铝',   'exchange': 'SHFE', 'unit': 5},
    'CU':  {'name': '沪铜',   'exchange': 'SHFE', 'unit': 5},
    'NI':  {'name': '沪镍',   'exchange': 'SHFE', 'unit': 1},
    'PB':  {'name': '沪铅',   'exchange': 'SHFE', 'unit': 5},
    'SN':  {'name': '沪锡',   'exchange': 'SHFE', 'unit': 1},
    'ZN':  {'name': '沪锌',   'exchange': 'SHFE', 'unit': 5},
    'AU':  {'name': '沪金',   'exchange': 'SHFE', 'unit': 1000},
    'AG':  {'name': '沪银',   'exchange': 'SHFE', 'unit': 15},
    'RB':  {'name': '螺纹钢', 'exchange': 'SHFE', 'unit': 10},
    'HC':  {'name': '热卷',   'exchange': 'SHFE', 'unit': 10},
    'SS':  {'name': '不锈钢', 'exchange': 'SHFE', 'unit': 5},
    'TA':  {'name': 'PTA',    'exchange': 'CZCE', 'unit': 5},
    'MA':  {'name': '甲醇',   'exchange': 'CZCE', 'unit': 10},
    'FG':  {'name': '玻璃',   'exchange': 'CZCE', 'unit': 20},
    'SA':  {'name': '纯碱',   'exchange': 'CZCE', 'unit': 20},
    'UR':  {'name': '尿素',   'exchange': 'CZCE', 'unit': 20},
    'CF':  {'name': '棉花',   'exchange': 'CZCE', 'unit': 5},
    'SR':  {'name': '白糖',   'exchange': 'CZCE', 'unit': 10},
    'I':   {'name': '铁矿石', 'exchange': 'DCE',  'unit': 100},
    'J':   {'name': '焦炭',   'exchange': 'DCE',  'unit': 100},
    'JM':  {'name': '焦煤',   'exchange': 'DCE',  'unit': 60},
    'PP':  {'name': '聚丙烯', 'exchange': 'DCE',  'unit': 5},
    'L':   {'name': '塑料',   'exchange': 'DCE',  'unit': 5},
    'EG':  {'name': '乙二醇', 'exchange': 'DCE',  'unit': 10},
    'V':   {'name': 'PVC',    'exchange': 'DCE',  'unit': 5},
    'EB':  {'name': '苯乙烯', 'exchange': 'DCE',  'unit': 5},
    'PG':  {'name': 'LPG',    'exchange': 'DCE',  'unit': 20},
    'SC':  {'name': '原油',   'exchange': 'INE',  'unit': 1000},
    'LU':  {'name': '低硫燃油','exchange': 'INE',  'unit': 10},
    'BC':  {'name': '国际铜', 'exchange': 'INE',  'unit': 5},
}

CONFIG_FILE = Path(__file__).parent / "web_config.json"

DEFAULT_CONFIG = {
    'baskets': [],
    'refresh_interval': 10,
}

# ==========================================
# 配置持久化
# ==========================================
def load_config():
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding='utf-8'))
        except Exception:
            pass
    return json.loads(json.dumps(DEFAULT_CONFIG))

def save_config(cfg):
    try:
        CONFIG_FILE.write_text(
            json.dumps(cfg, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as e:
        st.error(f"配置保存失败: {e}")

# ==========================================
# 数据获取层
# ==========================================
@st.cache_resource
def get_sessions():
    sina = requests.Session()
    sina.headers.update({
        'Referer': 'https://finance.sina.com.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    })
    tencent = requests.Session()
    tencent.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    })
    return sina, tencent

def sina_futures_code(commodity_code, month):
    return f"nf_{commodity_code}{month}"

def fetch_sina(baskets):
    sina, _ = get_sessions()
    codes = []
    for b in baskets:
        codes.append(b['etf_code'])
        codes.append(f"f_{b['fund_code']}")
        for c in b['contracts']:
            codes.append(sina_futures_code(c['code'], c['month']))
    if not codes:
        return {}
    url = f"https://hq.sinajs.cn/list={','.join(codes)}"
    resp = sina.get(url, timeout=10)
    resp.encoding = 'gbk'
    raw_map = {}
    for line in resp.text.strip().split('\n'):
        m = re.search(r'hq_str_(\w+)="(.*?)"', line)
        if m:
            raw_map[m.group(1)] = m.group(2)
    return raw_map

def fetch_iopv_tencent(etf_codes):
    _, tencent = get_sessions()
    if not etf_codes:
        return {}
    url = f"http://qt.gtimg.cn/q={','.join(etf_codes)}"
    resp = tencent.get(url, timeout=8)
    resp.encoding = 'gbk'
    result = {}
    for line in resp.text.strip().split(';'):
        line = line.strip()
        if not line:
            continue
        m = re.search(r'v_(\w+)="(.*?)"', line)
        if not m:
            continue
        fields = m.group(2).split('~')
        fund_code = fields[2] if len(fields) > 2 else ''
        iopv, nav = 0.0, 0.0
        if len(fields) > 78 and fields[78]:
            try:
                iopv = float(fields[78])
            except ValueError:
                pass
        if len(fields) > 81 and fields[81]:
            try:
                nav = float(fields[81])
            except ValueError:
                pass
        if fund_code:
            result[fund_code] = {'iopv': iopv, 'nav': nav}
    return result

def parse_futures(raw):
    if not raw:
        return None
    f = raw.split(',')
    if len(f) < 15:
        return None
    def sf(i):
        try:
            return float(f[i]) if i < len(f) and f[i] else 0.0
        except (ValueError, IndexError):
            return 0.0
    return {'name': f[0], 'open': sf(2), 'high': sf(3), 'low': sf(4),
            'last': sf(8), 'settle': sf(9), 'prev_settle': sf(10), 'volume': sf(14)}

def parse_etf(raw):
    if not raw:
        return None
    f = raw.split(',')
    if len(f) < 9:
        return None
    try:
        return {'name': f[0], 'price': float(f[3]) if f[3] else 0,
                'prev_close': float(f[2]) if f[2] else 0}
    except (ValueError, IndexError):
        return None

def parse_fund_nav(raw):
    if not raw:
        return None
    f = raw.split(',')
    if len(f) < 5:
        return None
    try:
        return {'name': f[0], 'nav': float(f[1]) if f[1] else 0,
                'nav_date': f[4] if len(f) > 4 else ''}
    except (ValueError, IndexError):
        return None

# ==========================================
# 计算引擎
# ==========================================
def calculate_premium(contracts, etf_price, etf_nav):
    total_value = 0
    for c in contracts:
        c['market_value'] = c['qty'] * c['unit'] * c['current_price']
        total_value += c['market_value']
    if total_value == 0:
        return {'weighted_premium': 0, 'est_nav': etf_nav,
                'overall_premium': 0, 'total_value': 0,
                'total_value_div095': 0, 'contracts': contracts}
    wp_sum = 0
    for c in contracts:
        c['weight'] = c['market_value'] / total_value
        c['premium'] = (c['current_price'] / c['ref_price'] - 1) if c['ref_price'] > 0 else 0
        c['weighted_premium'] = c['premium'] * c['weight']
        wp_sum += c['weighted_premium']
    est_nav = etf_nav * (1 + wp_sum) if etf_nav > 0 else 0
    overall = (etf_price / est_nav - 1) if est_nav > 0 and etf_price > 0 else 0
    return {
        'weighted_premium': wp_sum,
        'est_nav': est_nav,
        'overall_premium': overall,
        'total_value': total_value,
        'total_value_div095': total_value / 0.95,
        'contracts': contracts,
    }

# ==========================================
# 获取全部数据并计算
# ==========================================
def fetch_all_data(baskets):
    errors = []
    try:
        raw_map = fetch_sina(baskets)
    except Exception as e:
        return {}, f"网络请求失败: {e}"
    if not raw_map:
        return {}, "数据源返回为空，请检查网络连接"

    etf_codes = [b['etf_code'] for b in baskets]
    try:
        iopv_map = fetch_iopv_tencent(etf_codes)
    except Exception:
        iopv_map = {}

    results = {}
    for idx, basket in enumerate(baskets):
        try:
            etf_data = parse_etf(raw_map.get(basket['etf_code'], ''))
            nav_data = parse_fund_nav(raw_map.get(f"f_{basket['fund_code']}", ''))
            nav = basket.get('etf_nav', 0)
            
            # 这里的更新只会保存在内存中，不会触发文件写入死循环
            if nav <= 0 and nav_data and nav_data['nav'] > 0:
                nav = nav_data['nav']
                basket['etf_nav'] = nav 

            calc_contracts = []
            for c in basket['contracts']:
                key = sina_futures_code(c['code'], c['month'])
                fd = parse_futures(raw_map.get(key, ''))
                if not fd:
                    errors.append(f"{basket['name']}: {c['code']}{c['month']} 无行情")
                calc_contracts.append({
                    'code': c['code'], 'month': c['month'],
                    'qty': c['qty'], 'unit': c['unit'],
                    'current_price': fd['last'] if fd else 0,
                    'ref_price': fd['prev_settle'] if fd else 0,
                })

            iopv_info = iopv_map.get(basket['fund_code'], {})
            iopv_val = iopv_info.get('iopv', 0)
            etf_price = etf_data['price'] if etf_data else 0
            calc_result = calculate_premium(calc_contracts, etf_price, nav)
            iopv_premium = (etf_price / iopv_val - 1) if iopv_val > 0 and etf_price > 0 else 0

            results[idx] = {
                'etf_data': etf_data, 'nav_data': nav_data,
                'nav': nav, 'calc_result': calc_result,
                'iopv': iopv_val, 'iopv_premium': iopv_premium,
            }
        except Exception as e:
            errors.append(f"{basket.get('name', idx)}: {e}")
    return results, '; '.join(errors)

# ==========================================
# 提醒检测
# ==========================================
METRIC_NAMES = {'futures_premium': '期货估算溢价率', 'iopv_premium': 'IOPV溢价率'}

def check_alerts(basket, futures_prem_pct, iopv_prem_pct):
    alerts = basket.get('alerts', [])
    triggered = []
    now = time.time()
    cooldowns = st.session_state.setdefault('alert_cooldowns', {})
    basket_name = basket.get('name', '')

    for ai, a in enumerate(alerts):
        if not a.get('enabled', True):
            continue
        cd_key = f"{basket_name}_{ai}"
        if now - cooldowns.get(cd_key, 0) < 120:
            continue
        val = futures_prem_pct if a['metric'] == 'futures_premium' else iopv_prem_pct
        op, th = a['operator'], a['threshold']
        fired = (op == '>=' and val >= th) or (op == '<=' and val <= th)
        if fired:
            cooldowns[cd_key] = now
            triggered.append(
                f"**【{basket_name}】** {METRIC_NAMES.get(a['metric'])} "
                f"触发 {op} {th:+.2f}%，当前值: {val:+.4f}%")
    return triggered

# ==========================================
# 样式辅助
# ==========================================
def colored_pct(value, fmt="+.4f"):
    pct = value * 100
    if value > 0.001:
        color = "#FF5252"
    elif value < -0.001:
        color = "#4CAF50"
    else:
        color = "#FFC107"
    return f'<span style="color:{color};font-weight:bold;font-size:1.4em">{pct:{fmt}}%</span>'

def metric_card(label, value_html):
    return f"""
    <div style="text-align:center;padding:6px 12px">
        <div style="color:rgba(255,255,255,0.55);font-size:0.78em;margin-bottom:2px">{label}</div>
        <div>{value_html}</div>
    </div>"""

# ==========================================
# 主界面
# ==========================================
def main():
    if 'config' not in st.session_state:
        st.session_state.config = load_config()
    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = False

    cfg = st.session_state.config
    baskets = cfg.get('baskets', [])

    st.markdown('<h2 style="text-align:center;margin-bottom:0">🐼 商品期货ETF套利监测系统（BBS手作）</h2>', unsafe_allow_html=True)

    ctrl_cols = st.columns([1, 1, 2, 1])
    with ctrl_cols[0]:
        interval = st.number_input("刷新间隔(秒)", min_value=5, max_value=300, value=cfg.get('refresh_interval', 10), step=1, key="interval_input")
    with ctrl_cols[1]:
        auto = st.toggle("自动刷新", value=st.session_state.auto_refresh, key="auto_toggle")
        st.session_state.auto_refresh = auto
    with ctrl_cols[2]:
        st.markdown(f"<br><small style='color:#888'>最后更新: {datetime.now().strftime('%H:%M:%S')}</small>", unsafe_allow_html=True)
    with ctrl_cols[3]:
        st.markdown("<br>", unsafe_allow_html=True)
        manual_refresh = st.button("🔄 手动刷新", width='stretch')

    # ---- 获取数据 ----
    results, err_msg = fetch_all_data(baskets)
    if err_msg:
        st.warning(f"数据异常: {err_msg}")

    # 注意：这里删除了之前无条件调用的 save_config(cfg)，防止触发死循环

    all_triggered = []

    if not baskets:
        st.info("暂无套利篮子，请在侧边栏配置。")
    else:
        tab_names = [b['name'] for b in baskets]
        tabs = st.tabs(tab_names)

        for idx, (tab, basket) in enumerate(zip(tabs, baskets)):
            with tab:
                r = results.get(idx)
                if not r:
                    st.error(f"篮子 [{basket['name']}] 数据获取失败")
                    continue

                etf_data = r['etf_data']
                nav = r['nav']
                calc = r['calc_result']
                iopv_val = r['iopv']
                iopv_prem = r['iopv_premium']
                overall = calc.get('overall_premium', 0)
                est_nav = calc.get('est_nav', 0)
                nav_data = r.get('nav_data')
                nav_date = nav_data.get('nav_date', '') if nav_data else ''

                etf_price = etf_data['price'] if etf_data else 0
                iopv_src = "腾讯IOPV" if iopv_val > 0 else "IOPV未获取"

                st.markdown(f"""
                <div style="background:linear-gradient(135deg,#1a237e,#283593);
                    border-radius:10px;padding:16px 20px;margin-bottom:12px">
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                        <span style="color:rgba(255,255,255,0.7);font-size:0.9em">
                            {basket['name']} · {basket.get('etf_name','')} ({basket['fund_code']})</span>
                        <span style="color:rgba(255,255,255,0.5);font-size:0.8em">
                            昨日净值: {nav:.4f} ({nav_date}) &nbsp; 数据源: {iopv_src}</span>
                    </div>
                    <div style="display:flex;gap:24px;flex-wrap:wrap">
                        {metric_card("ETF 现价", f'<span style="color:#fff;font-weight:bold;font-size:1.4em">{etf_price:.4f}</span>')}
                        {metric_card("IOPV 参考净值", f'<span style="color:#80DEEA;font-weight:bold;font-size:1.4em">{iopv_val:.4f}</span>' if iopv_val > 0 else '<span style="color:#666">--</span>')}
                        {metric_card("期货估算净值", f'<span style="color:#90CAF9;font-weight:bold;font-size:1.4em">{est_nav:.4f}</span>' if est_nav > 0 else '<span style="color:#666">--</span>')}
                        {metric_card("IOPV 溢价", colored_pct(iopv_prem))}
                        {metric_card("期货估算溢价", colored_pct(overall))}
                    </div>
                </div>""", unsafe_allow_html=True)

                contracts = calc.get('contracts', [])
                if contracts:
                    rows = []
                    for c in contracts:
                        cdb = COMMODITY_DB.get(c['code'], {})
                        name = f"{cdb.get('name', c['code'])}{c['month']}"
                        rows.append({
                            '合约': name, '数量': c['qty'], '单位': c['unit'],
                            '现价': f"{c['current_price']:.1f}" if c['current_price'] > 0 else "--",
                            '昨结算': f"{c['ref_price']:.1f}" if c['ref_price'] > 0 else "--",
                            '溢价%': f"{c.get('premium',0)*100:+.4f}",
                            '加权溢价%': f"{c.get('weighted_premium',0)*100:+.4f}",
                            '市值': f"{c.get('market_value',0):,.0f}",
                            '占比%': f"{c.get('weight',0)*100:.2f}",
                        })
                    df = pd.DataFrame(rows)
                    st.dataframe(df, width='stretch', hide_index=True)

                tv = calc.get('total_value', 0)
                tv095 = calc.get('total_value_div095', 0)
                wp = calc.get('weighted_premium', 0)
                sum_cols = st.columns(3)
                sum_cols[0].markdown(f"**总市值:** {tv:,.0f}")
                sum_cols[1].markdown(f"**÷0.95:** {tv095:,.0f}")
                sum_cols[2].markdown(f"**期货加权变动:** {wp*100:+.4f}%")

                st.markdown("---")
                st.markdown("##### 溢价提醒规则")
                alerts = basket.setdefault('alerts', [])
                if alerts:
                    for ai, a in enumerate(alerts):
                        a_cols = st.columns([3, 2, 2, 1, 1, 1])
                        metric_label = METRIC_NAMES.get(a['metric'], a['metric'])
                        a_cols[0].markdown(f"**{metric_label}**")
                        a_cols[1].markdown(f"`{a['operator']}`")
                        a_cols[2].markdown(f"`{a['threshold']:+.2f}%`")
                        status = "✅ 启用" if a.get('enabled', True) else "⏸ 禁用"
                        a_cols[3].markdown(status)
                        if a_cols[4].button("禁/启", key=f"toggle_{idx}_{ai}"):
                            a['enabled'] = not a.get('enabled', True)
                            save_config(cfg)
                            st.rerun()
                        if a_cols[5].button("🗑️", key=f"del_{idx}_{ai}"):
                            alerts.pop(ai)
                            save_config(cfg)
                            st.rerun()
                else:
                    st.caption("暂无提醒规则")

                with st.expander("➕ 添加新提醒规则"):
                    new_cols = st.columns(4)
                    new_metric = new_cols[0].selectbox("监控指标", ["期货估算溢价率", "IOPV溢价率"], key=f"new_metric_{idx}")
                    new_op = new_cols[1].selectbox("条件", [">=（大于等于）", "<=（小于等于）"], key=f"new_op_{idx}")
                    new_th = new_cols[2].number_input("阈值(%)", min_value=-50.0, max_value=50.0, value=1.0, step=0.1, format="%.2f", key=f"new_th_{idx}")
                    new_cols[3].markdown("<br>", unsafe_allow_html=True)
                    if new_cols[3].button("确认添加", key=f"add_alert_{idx}"):
                        alerts.append({
                            'metric': 'futures_premium' if '期货' in new_metric else 'iopv_premium',
                            'operator': '>=' if '>=' in new_op else '<=',
                            'threshold': new_th,
                            'enabled': True,
                        })
                        save_config(cfg)
                        st.rerun()

                futures_prem_pct = overall * 100
                iopv_prem_pct = iopv_prem * 100
                triggered = check_alerts(basket, futures_prem_pct, iopv_prem_pct)
                all_triggered.extend(triggered)

    if all_triggered:
        for msg in all_triggered:
            st.toast(f"⚠️ {msg}", icon="🚨")
        st.warning("⚠️ **溢价提醒触发！**\n\n" + "\n\n".join(all_triggered))

    with st.sidebar:
        st.header("篮子配置")
        for idx, basket in enumerate(baskets):
            with st.expander(f"📦 {basket['name']}", expanded=False):
                basket['name'] = st.text_input("篮子名称", value=basket['name'], key=f"bname_{idx}")
                basket['etf_code'] = st.text_input("ETF代码", value=basket['etf_code'], key=f"betf_{idx}")
                basket['fund_code'] = st.text_input("基金代码", value=basket['fund_code'], key=f"bfund_{idx}")
                basket['etf_name'] = st.text_input("ETF名称", value=basket.get('etf_name', ''), key=f"betfn_{idx}")
                nav_val = st.number_input("昨日净值", value=float(basket.get('etf_nav', 0)), format="%.4f", step=0.0001, key=f"bnav_{idx}")
                basket['etf_nav'] = nav_val

                st.markdown("**合约列表:**")
                new_contracts = []
                for ci, c in enumerate(basket['contracts']):
                    cc = st.columns([2, 2, 1, 1, 1])
                    code = cc[0].text_input("品种", value=c['code'], key=f"cc_{idx}_{ci}")
                    month = cc[1].text_input("月份", value=c['month'], key=f"cm_{idx}_{ci}")
                    qty = cc[2].number_input("数量", value=c['qty'], min_value=1, key=f"cq_{idx}_{ci}")
                    unit = cc[3].number_input("单位", value=c['unit'], min_value=1, key=f"cu_{idx}_{ci}")
                    keep = cc[4].checkbox("保留", value=True, key=f"ck_{idx}_{ci}")
                    if keep:
                        new_contracts.append({'code': code, 'month': month, 'qty': qty, 'unit': unit})
                basket['contracts'] = new_contracts

                add_c = st.columns(3)
                new_code = add_c[0].text_input("新品种", key=f"nc_{idx}")
                new_month = add_c[1].text_input("新月份", key=f"nm_{idx}")
                if add_c[2].button("添加合约", key=f"ac_{idx}"):
                    if new_code and new_month:
                        db_info = COMMODITY_DB.get(new_code.upper(), {})
                        basket['contracts'].append({'code': new_code.upper(), 'month': new_month, 'qty': 1, 'unit': db_info.get('unit', 1)})
                        save_config(cfg)
                        st.rerun()

                if st.button(f"🗑️ 删除篮子 [{basket['name']}]", key=f"delb_{idx}"):
                    baskets.pop(idx)
                    save_config(cfg)
                    st.rerun()

        st.markdown("---")
        if st.button("➕ 新建篮子"):
            baskets.append({'name': '新篮子', 'etf_code': 'sz', 'etf_name': '', 'fund_code': '', 'etf_nav': 0, 'contracts': [], 'alerts': []})
            save_config(cfg)
            st.rerun()

        if st.button("💾 保存所有配置"):
            save_config(cfg)
            st.success("配置已保存！")

    if st.session_state.auto_refresh:
        time.sleep(interval)
        st.rerun()

if __name__ == "__main__":
    main()
