#!/usr/bin/env python3
"""
CFTC Positioning Replicator
============================
复制 JPM Delta-One Table 12: Traders in Financial Futures & COT Disaggregated

数据来源: CFTC Socrata API (免费, 无需API key)
输出: 单一HTML, 聚焦 Leveraged Funds (TFF) / Managed Money (Disagg)
     含多头/空头/净持仓的 position, z-score, w/w change

用法:
    python3 cftc.py                            # 最新一期
    python3 cftc.py --date 2026-03-17          # 指定日期
    python3 cftc.py --output my_report.html    # 自定义输出文件名
    python3 cftc.py --no-price                 # 跳过价格获取（网络受限时）
"""

import pandas as pd
import numpy as np
import requests
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from html import escape
import sys
import warnings
import time
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

CFTC_TFF_URL    = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
CFTC_DISAGG_URL = "https://publicreporting.cftc.gov/resource/kh3c-gbw2.json"
LOOKBACK_DAYS   = 1200   # ~3.3年, 确保有足够历史数据
ZSCORE_WINDOW   = 52    # 3年 = 156周
MAX_PRICE_WORKERS = 6    # 并发价格拉取线程数

# ============================================================================
# CONTRACT MAPPINGS
# ============================================================================

TFF_CONTRACTS = [
    # 股指
    {'name': '标普500',      'cftc': 'E-MINI S&P 500 -',          'section': '股指',      'yf': '^GSPC'},
    {'name': '纳斯达克100',  'cftc': 'NASDAQ MINI',                'section': '股指',      'yf': '^NDX'},
    {'name': '罗素2000',     'cftc': 'RUSSELL E-MINI',             'section': '股指',      'yf': '^RUT'},
    {'name': 'MSCI新兴市场', 'cftc': 'MSCI EM INDEX',              'section': '股指',      'yf': 'EEM'},
    {'name': 'MSCI发达市场', 'cftc': 'MSCI EAFE',                  'section': '股指',      'yf': 'EFA'},
    {'name': '日经225',      'cftc': 'NIKKEI STOCK AVERAGE',       'section': '股指',      'yf': '^N225'},
    # 债券
    {'name': '2年期美债',    'cftc': 'UST 2Y NOTE',                'section': '债券',      'yf': 'ZT=F'},
    {'name': '10年期美债',   'cftc': 'UST 10Y NOTE',               'section': '债券',      'yf': 'ZN=F'},
    {'name': '超长期美债',   'cftc': 'ULTRA UST BOND',             'section': '债券',      'yf': 'UB=F'},
    # 利率
    {'name': '联邦基金',     'cftc': 'FED FUNDS',                  'section': '利率',      'yf': 'ZQ=F'},
    # 外汇/加密
    {'name': '欧元/美元',    'cftc': 'EURO FX - CHICAGO',          'section': '外汇/加密', 'yf': 'EURUSD=X'},
    {'name': '英镑/美元',    'cftc': 'BRITISH POUND',              'section': '外汇/加密', 'yf': 'GBPUSD=X'},
    {'name': '日元/美元',    'cftc': 'JAPANESE YEN',               'section': '外汇/加密', 'yf': 'JPYUSD=X'},
    {'name': '澳元/美元',    'cftc': 'AUSTRALIAN DOLLAR',          'section': '外汇/加密', 'yf': 'AUDUSD=X'},
    {'name': '比特币',       'cftc': 'BITCOIN - CHICAGO MERCANTILE','section': '外汇/加密','yf': 'BTC-USD'},
]

DISAGG_CONTRACTS = [
    {'name': 'WTI原油', 'cftc': 'WTI-PHYSICAL',       'section': '能源',   'yf': 'CL=F'},
    {'name': '天然气',  'cftc': 'NAT GAS NYME',        'section': '能源',   'yf': 'NG=F'},
    {'name': '铜',      'cftc': 'COPPER- #1',          'section': '金属',   'yf': 'HG=F'},
    {'name': '黄金',    'cftc': 'GOLD - COMMODITY',    'section': '金属',   'yf': 'GC=F'},
    {'name': '白银',    'cftc': 'SILVER - COMMODITY',  'section': '金属',   'yf': 'SI=F'},
    {'name': '玉米',    'cftc': 'CORN - CHICAGO',      'section': '农产品', 'yf': 'ZC=F'},
]

# ============================================================================
# UTILITIES
# ============================================================================

def _is_nan(v):
    """统一 None / float NaN 检测"""
    return v is None or (isinstance(v, float) and np.isnan(v))


# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_cftc(endpoint, start_date, limit=50000):
    """从CFTC Socrata API获取数据，支持分页，含重试和指数退避。

    CFTC Socrata 单次最多返回 50000 行；若实际行数等于 limit，
    说明可能被截断，自动翻页直到取完所有数据。
    """
    params_base = {
        "$where": f"report_date_as_yyyy_mm_dd >= '{start_date}'",
        "$order": "report_date_as_yyyy_mm_dd ASC",
    }
    max_attempts = 5
    all_frames = []
    offset = 0

    while True:
        params = {**params_base, "$limit": limit, "$offset": offset}
        resp = None
        for attempt in range(max_attempts):
            try:
                resp = requests.get(endpoint, params=params, timeout=120)
                resp.raise_for_status()
                break
            except requests.exceptions.SSLError as e:
                # SSL 握手失败时降级为不验证证书重试一次（仅限 CFTC 官方域名）
                if attempt < max_attempts - 1:
                    wait = 2 ** attempt
                    print(f"    [RETRY {attempt+1}/{max_attempts-1}] SSLError, "
                          f"retrying with verify=False in {wait}s...")
                    time.sleep(wait)
                    try:
                        resp = requests.get(endpoint, params=params,
                                            timeout=120, verify=False)
                        resp.raise_for_status()
                        break
                    except Exception:
                        pass   # 降级也失败，继续外层重试
                else:
                    print(f"    [FAILED] All {max_attempts} attempts exhausted.")
                    raise
            except requests.exceptions.RequestException as e:
                if attempt < max_attempts - 1:
                    wait = 2 ** attempt
                    print(f"    [RETRY {attempt+1}/{max_attempts-1}] {type(e).__name__}, "
                          f"retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"    [FAILED] All {max_attempts} attempts exhausted.")
                    raise

        batch = pd.DataFrame(resp.json())
        if batch.empty:
            break
        all_frames.append(batch)
        fetched = len(batch)
        print(f"    offset={offset}: {fetched} rows")
        if fetched < limit:
            break           # 最后一页，无需继续
        offset += limit     # 翻页

    if not all_frames:
        return pd.DataFrame()

    df = pd.concat(all_frames, ignore_index=True)

    # 去重（多次分页可能有极少量重叠）
    df = df.drop_duplicates()

    skip_cols = {
        'market_and_exchange_names', 'report_date_as_yyyy_mm_dd',
        'cftc_contract_market_code', 'cftc_market_code', 'cftc_commodity_code',
        'cftc_region_code', 'cftc_subgroup_code', 'contract_market_name',
        'contract_units', 'futonly_or_combined', 'id', 'commodity',
        'commodity_group_name', 'commodity_name', 'commodity_subgroup_name',
        'report_date_as_mm_dd_yyyy', 'yyyy_report_week_ww',
    }
    for col in df.columns:
        if col not in skip_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df['report_date'] = pd.to_datetime(df['report_date_as_yyyy_mm_dd'])
    return df


def match_cftc(df, search_pattern):
    """在CFTC数据中按名称匹配合约"""
    if search_pattern is None:
        return None
    names_upper    = df['market_and_exchange_names'].str.upper()
    pattern_upper  = search_pattern.upper()

    mask = names_upper == pattern_upper
    if not mask.any():
        mask = names_upper.str.startswith(pattern_upper, na=False)
    if not mask.any():
        mask = df['market_and_exchange_names'].str.contains(search_pattern, case=False, na=False)

    matched = df[mask].copy()
    if matched.empty:
        return None

    # 多个交易所名称 → 优先 Consolidated，否则选 OI 最大
    if matched['market_and_exchange_names'].nunique() > 1:
        names = matched['market_and_exchange_names'].unique()
        for n in names:
            if 'Consolidated' in n:
                matched = matched[matched['market_and_exchange_names'] == n]
                break
        else:
            avg_oi = matched.groupby('market_and_exchange_names')['open_interest_all'].mean()
            matched = matched[matched['market_and_exchange_names'] == avg_oi.idxmax()]

    # 多个合约代码 → 选 OI 最大
    if 'cftc_contract_market_code' in matched.columns and matched['cftc_contract_market_code'].nunique() > 1:
        avg_oi = matched.groupby('cftc_contract_market_code')['open_interest_all'].mean()
        matched = matched[matched['cftc_contract_market_code'] == avg_oi.idxmax()]

    return matched.sort_values('report_date').reset_index(drop=True)


# ============================================================================
# PROCESSING
# ============================================================================

def calc_zscore(series, window=ZSCORE_WINDOW):
    s = series.dropna()
    if len(s) < 10:
        return np.nan
    tail = s.tail(window)
    mean, std = tail.mean(), tail.std()
    if std == 0 or np.isnan(std):
        return 0.0
    return round((s.iloc[-1] - mean) / std, 1)


def calc_change_zscore(series, window=ZSCORE_WINDOW):
    changes = series.diff().dropna()
    if len(changes) < 10:
        return np.nan
    tail = changes.tail(window)
    mean, std = tail.mean(), tail.std()
    if std == 0 or np.isnan(std):
        return 0.0
    return round((changes.iloc[-1] - mean) / std, 1)


def _pos_group(matched, long_col, short_col):
    """计算一组持仓的 net/long/short 的 position, z-score, w/w change
    z-score 使用 OI 归一化后的占比（与 toggle chart 口径一致）"""
    long_s  = matched[long_col].fillna(0)
    short_s = matched[short_col].fillna(0)
    net_s   = long_s - short_s
    oi      = matched['open_interest_all'].fillna(0).replace(0, np.nan)

    long_oi = long_s / oi
    short_oi = short_s / oi
    net_oi  = net_s / oi

    z_dlong  = calc_change_zscore(long_s)
    z_dshort = calc_change_zscore(short_s)

    return {
        'net':        int(net_s.iloc[-1]),
        'net_z':      calc_zscore(net_oi),
        'net_ww':     int(net_s.diff().iloc[-1])  if len(net_s)   > 1 else 0,
        'net_ww_z':   calc_change_zscore(net_s),
        'long':       int(long_s.iloc[-1]),
        'long_z':     calc_zscore(long_oi),
        'long_ww':    int(long_s.diff().iloc[-1]) if len(long_s)  > 1 else 0,
        'long_ww_z':  z_dlong,
        'short':      int(short_s.iloc[-1]),
        'short_z':    calc_zscore(short_oi),
        'short_ww':   int(short_s.diff().iloc[-1]) if len(short_s) > 1 else 0,
        'short_ww_z': z_dshort,
        'flow_state': _flow_state(z_dlong, z_dshort),
    }


def _flow_state(z_dlong, z_dshort):
    """根据多空变化z-score判定flow state"""
    if _is_nan(z_dlong) or _is_nan(z_dshort):
        return ''
    zl, zs = float(z_dlong), float(z_dshort)

    # 双向极端 (优先判定)
    if zl >= 0.8 and zs <= -0.8:  return '多头挤压'
    if zl <= -0.8 and zs >= 0.8:  return '空头施压'
    if zl >= 0.8 and zs >= 0.8:   return '多空双增'
    if zl <= -0.8 and zs <= -0.8: return '多空双减'
    # 单向主导
    if zl >= 0.8  and abs(zs) < 0.5: return '多头建仓'
    if zs <= -0.8 and abs(zl) < 0.5: return '空头回补'
    if zs >= 0.8  and abs(zl) < 0.5: return '空头建仓'
    if zl <= -0.8 and abs(zs) < 0.5: return '多头平仓'
    return ''


def _fetch_single_price(name, ticker, _unused_start, _unused_end, tue_start, tue_end):
    """拉取单个 ticker 的 Tue→Tue 价格。

    使用 yf.Ticker.history() 代替 yf.download()：
      - 始终返回扁平 DataFrame（无 MultiIndex），兼容所有 yfinance 版本
      - 宽裕窗口 ±14 天覆盖长假
      - 统一处理 tz-aware 索引
      - 对价格做合理性校验（非零、非 NaN、涨跌幅 < 50%）
    """
    import random

    wide_start = (tue_start - timedelta(days=14)).strftime('%Y-%m-%d')
    wide_end   = (tue_end   + timedelta(days=5)).strftime('%Y-%m-%d')

    # tz-naive 基准，用于日期比较
    ts_end   = pd.Timestamp(tue_end).tz_localize(None)
    ts_start = pd.Timestamp(tue_start).tz_localize(None)

    max_attempts = 4
    for attempt in range(max_attempts):
        try:
            if attempt == 0:
                time.sleep(random.uniform(0, 0.5))

            # Ticker.history() 返回扁平 DataFrame，Close 列始终是 Series
            hist = yf.Ticker(ticker).history(
                start=wide_start,
                end=wide_end,
                auto_adjust=True,
                raise_errors=False,
            )

            if hist is None or hist.empty or 'Close' not in hist.columns:
                if attempt < max_attempts - 1:
                    wait = 2 ** attempt
                    print(f"    [PRICE WARN] {name} ({ticker}): empty, retry in {wait}s")
                    time.sleep(wait)
                    continue
                print(f"    [PRICE FAIL] {name} ({ticker}): no data after {max_attempts} attempts")
                return name, None

            close = hist['Close'].dropna()
            if close.empty:
                if attempt < max_attempts - 1:
                    wait = 2 ** attempt
                    print(f"    [PRICE WARN] {name} ({ticker}): all NaN, retry in {wait}s")
                    time.sleep(wait)
                    continue
                return name, None

            # 统一去除时区
            if hasattr(close.index, 'tz') and close.index.tz is not None:
                close.index = close.index.tz_localize(None)

            px_end_s   = close[close.index <= ts_end]
            px_start_s = close[close.index <= ts_start]

            if px_end_s.empty or px_start_s.empty:
                print(f"    [PRICE WARN] {name} ({ticker}): "
                      f"end_rows={len(px_end_s)} start_rows={len(px_start_s)}, "
                      f"data {close.index[0].date()}~{close.index[-1].date()}")
                return name, None

            p1 = float(px_start_s.iloc[-1])
            p2 = float(px_end_s.iloc[-1])

            # 合理性校验：价格必须为正，且单周涨跌幅不超过 ±50%
            if p1 <= 0 or p2 <= 0 or abs(p2 / p1 - 1) > 0.5:
                print(f"    [PRICE WARN] {name} ({ticker}): "
                      f"suspicious values p1={p1:.4f} p2={p2:.4f}, skip")
                return name, None

            return name, {
                'ret':        round((p2 / p1 - 1) * 100, 2),
                'ticker':     ticker,
                'date_start': px_start_s.index[-1].strftime('%m/%d'),
                'date_end':   px_end_s.index[-1].strftime('%m/%d'),
                'px_start':   p1,
                'px_end':     p2,
                'px_cur':     p2,
            }

        except Exception as e:
            if attempt < max_attempts - 1:
                wait = 2 ** attempt
                print(f"    [PRICE WARN] {name} ({ticker}): {type(e).__name__}: {e}, retry in {wait}s")
                time.sleep(wait)
            else:
                print(f"    [PRICE FAIL] {name} ({ticker}): {type(e).__name__}: {e}")

    return name, None

    return name, None


def fetch_tue_tue_returns(contracts, cftc_date):
    """并发获取 CFTC 同期 Tue→Tue 价格变动。

    第一轮并发（限 4 线程），失败的 ticker 第二轮串行补拉。
    """
    tue_end   = pd.Timestamp(cftc_date)
    tue_start = tue_end - timedelta(days=7)

    tickers = {c['name']: c['yf'] for c in contracts if c.get('yf')}
    results  = {}
    failed   = []

    # 第一轮：并发（限 4 线程，降低限速风险）
    with ThreadPoolExecutor(max_workers=min(MAX_PRICE_WORKERS, 4)) as pool:
        futures = {
            pool.submit(_fetch_single_price, name, ticker,
                        None, None, tue_start, tue_end): (name, ticker)
            for name, ticker in tickers.items()
        }
        for fut in as_completed(futures):
            name, info = fut.result()
            if info is not None:
                results[name] = info
            else:
                failed.append(futures[fut])   # (name, ticker)

    # 第二轮：串行补拉失败的 ticker
    if failed:
        print(f"\n  [PRICE RETRY] 串行补拉 {len(failed)} 个失败 ticker...")
        for name, ticker in failed:
            time.sleep(3)
            # 注意：传 None 占位，函数内部会自行计算窗口
            _, info = _fetch_single_price(name, ticker, None, None, tue_start, tue_end)
            if info is not None:
                results[name] = info
                print(f"    -> {name} 补拉成功")
            else:
                print(f"    -> {name} 最终失败，报告中该列留空")

    total = len(tickers)
    ok    = len(results)
    print(f"  价格获取完成: {ok}/{total} 成功" +
          (f"，{total - ok} 个失败（见上方 PRICE FAIL 日志）" if ok < total else "，全部成功"))
    return results


def build_table(df_cftc, contracts, long_col, short_col, price_data=None):
    """通用持仓表构建（TFF / Disagg 共用）"""
    rows = []
    for c in contracts:
        matched = match_cftc(df_cftc, c['cftc'])
        if matched is None or matched.empty:
            continue
        row = _pos_group(matched, long_col, short_col)
        row['Instrument'] = c['name']
        row['_section']   = c['section']
        pd_info = price_data.get(c['name']) if price_data else None
        row['price_chg']  = pd_info['ret']    if pd_info else None
        row['price_cur']  = pd_info['px_cur'] if pd_info else None
        rows.append(row)
    return pd.DataFrame(rows)


def build_table12_tff(df_tff, contracts, price_data=None):
    return build_table(df_tff, contracts,
                       'lev_money_positions_long', 'lev_money_positions_short',
                       price_data)


def build_table12_disagg(df_disagg, contracts, price_data=None):
    return build_table(df_disagg, contracts,
                       'm_money_positions_long_all', 'm_money_positions_short_all',
                       price_data)


# ============================================================================
# HTML OUTPUT
# ============================================================================

CSS = """
:root {
    --blue: #4472C4; --blue-dark: #3461a8; --blue-light: #D6E4F0;
    --orange: #C55A11;
    --green-txt: #1a6b1a; --red-txt: #9C0006;
    --gray-border: #ddd; --row-alt: #f7f8fa;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, 'Helvetica Neue', Arial, sans-serif;
    font-size: 12px; color: #222;
    background: #f0f2f5; padding: 16px 20px;
}
header {
    border-bottom: 3px solid var(--orange);
    padding-bottom: 10px; margin-bottom: 18px;
    display: flex; justify-content: space-between; align-items: flex-end;
}
header h1 { font-size: 20px; font-weight: 700; color: var(--orange); }
header .meta { font-size: 11px; color: #777; text-align: right; line-height: 1.6; }

.section-label {
    font-size: 13px; font-weight: 600; color: var(--orange);
    margin: 20px 0 6px;
}

/* ── table ── */
table { border-collapse: collapse; width: 100%; font-size: 11.5px; background: #fff;
        box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 6px; }

/* header rows */
thead th {
    background: var(--blue); color: #fff; font-weight: 600;
    padding: 6px 7px; text-align: center;
    border: 1px solid var(--blue-dark); white-space: nowrap;
}
thead th:first-child { text-align: left; min-width: 90px; }
thead th.gh { background: var(--blue-dark); border-bottom: 2px solid var(--orange); }
thead tr.sub th {
    background: var(--blue); font-weight: 400; font-size: 11px;
    padding: 4px 6px;
}

/* body */
tbody td {
    padding: 4px 7px; border: 1px solid var(--gray-border);
    text-align: right; white-space: nowrap;
}
tbody td:first-child { text-align: left; font-weight: 600; min-width: 90px; background: #fafafa; }
tbody tr:nth-child(even) td { background: var(--row-alt); }
tbody tr:nth-child(even) td:first-child { background: #f3f4f7; }
tbody tr:hover td { background: #e8eef8 !important; }

/* section separator */
.sec-row td {
    background: var(--blue-light) !important;
    color: #1a3a6e; font-weight: 700; font-size: 11.5px;
    padding: 5px 8px; border: 1px solid #c5d8ed;
}

/* numbers */
.pos { color: var(--green-txt); }
.neg { color: var(--red-txt); }

/* z-bar cell */
.zbar { position: relative; min-width: 48px; padding: 0 !important;
        text-align: center !important; overflow: hidden; }
.zbar-fill { position: absolute; top: 2px; bottom: 2px; opacity: .28; pointer-events: none; }
.zbar-fill.zp { background: #00B050; left: 50%; }
.zbar-fill.zn { background: #e00; right: 50%; }
.zbar-lbl { position: relative; z-index: 1; font-size: 11px; font-weight: 700;
            padding: 3px 2px; display: block; }

/* price cell */
.price-cell { min-width: 56px; font-weight: 600; }
.diverg-price { background: #fff3cd !important; border: 2px solid #ffca2c !important; }

/* ── action / crowding badges ── */
.badge {
    display: inline-block; padding: 3px 10px; border-radius: 4px;
    font-size: 11px; font-weight: 700; white-space: nowrap;
    letter-spacing: .3px;
}
/* 看多类 */
.b-bull   { background: #c6efce; color: #1a6b1a; }
/* 看空类 */
.b-bear   { background: #ffc7ce; color: #9C0006; }
/* 混合 */
.b-mixed  { background: #fff2cc; color: #7f6000; }
/* 拥挤 */
.b-crowd  { background: #fce4d6; color: #C55A11; }
/* 极端 */
.b-ext    { background: #e84040; color: #fff; }

td.action-cell, td.crowd-cell {
    text-align: center !important; min-width: 72px;
}

/* source / notes */
.source { font-size: 10px; color: #aaa; margin-top: 3px; }
.notes {
    font-size: 11px; color: #666; margin-top: 18px; padding: 10px 14px;
    background: #fff; border-radius: 4px; border: 1px solid var(--gray-border);
    box-shadow: 0 1px 3px rgba(0,0,0,.06);
}
.notes ul { margin: 5px 0 0 16px; } .notes li { margin-bottom: 3px; }

/* price detail table */
.detail-wrap { background: #fff; padding: 12px 16px; margin-top: 16px;
               border-radius: 4px; border: 1px solid var(--gray-border); }
.detail-wrap h3 { font-size: 13px; color: var(--orange); margin-bottom: 8px; font-weight: 600; }

@media print { body { background: #fff; padding: 8px; } }
"""


# ── HTML helpers ────────────────────────────────────────────────────────────

def _zbar(val):
    if _is_nan(val):
        return '<td class="zbar"><span class="zbar-lbl"></span></td>'
    v   = float(val)
    pct = min(abs(v) / 3.0 * 50, 50)
    if v > 0:
        bar = f'<div class="zbar-fill zp" style="width:{pct:.0f}%"></div>'
        lbl = 'pos'
    elif v < 0:
        bar = f'<div class="zbar-fill zn" style="width:{pct:.0f}%"></div>'
        lbl = 'neg'
    else:
        bar, lbl = '', ''
    return f'<td class="zbar">{bar}<span class="zbar-lbl {lbl}">{v:+.1f}</span></td>'


def _chg_td(chg, z):
    if _is_nan(chg):
        return '<td></td>'
    chg_s = f'{int(chg):,}'
    z_s   = f'{float(z):.1f}z' if not _is_nan(z) else ''
    cls   = 'pos' if chg > 0 else ('neg' if chg < 0 else '')
    txt   = f'{chg_s} ({z_s})' if z_s else chg_s
    return f'<td class="{cls}">{txt}</td>'


def _num_td(val):
    if _is_nan(val):
        return '<td></td>'
    cls = 'pos' if val > 0 else ('neg' if val < 0 else '')
    return f'<td class="{cls}">{int(val):,}</td>'


def _flow_tag(state):
    """动作 badge — 绿/红/黄"""
    if not state:
        return '<td class="action-cell"></td>'
    bull  = {'多头建仓', '空头回补', '多头挤压'}
    bear  = {'空头建仓', '多头平仓', '空头施压'}
    cls   = 'b-bull' if state in bull else ('b-bear' if state in bear else 'b-mixed')
    return f'<td class="action-cell"><span class="badge {cls}">{escape(state)}</span></td>'


def _crowding_tag(net_z, long_z=None, short_z=None):
    """拥挤度 badge"""
    def _s(v): return 0.0 if _is_nan(v) else float(v)
    nz, lz, sz = _s(net_z), _s(long_z), _s(short_z)
    if nz >= 2.0 or lz >= 2.0:
        if nz >= 2.75 or lz >= 2.75:
            return '<td class="crowd-cell"><span class="badge b-ext">极端多头</span></td>'
        return '<td class="crowd-cell"><span class="badge b-crowd">拥挤多头</span></td>'
    if nz <= -2.0 or sz >= 2.0:
        if nz <= -2.75 or sz >= 2.75:
            return '<td class="crowd-cell"><span class="badge b-ext">极端空头</span></td>'
        return '<td class="crowd-cell"><span class="badge b-crowd">拥挤空头</span></td>'
    return '<td class="crowd-cell"></td>'


def _is_divergence(flow_state, price_chg):
    if not flow_state or _is_nan(price_chg):
        return False
    bull = {'多头建仓', '空头回补', '多头挤压'}
    bear = {'空头建仓', '多头平仓', '空头施压'}
    if flow_state in bull and price_chg < -0.05:
        return True
    if flow_state in bear and price_chg > 0.05:
        return True
    return False


def _cur_price_td(val):
    if _is_nan(val):
        return '<td class="price-cell"></td>'
    decimals = 4 if val < 1 else (1 if val >= 1000 else 2)
    return f'<td class="price-cell" style="color:#444">{val:,.{decimals}f}</td>'


def _price_td(val, diverge=False):
    if _is_nan(val):
        return '<td class="price-cell"></td>'
    cls  = 'pos' if val > 0 else ('neg' if val < 0 else '')
    div  = ' diverg-price' if diverge else ''
    return f'<td class="price-cell{div} {cls}">{val:+.1f}%</td>'


def _row_html(r):
    diverge = _is_divergence(r.get('flow_state', ''), r.get('price_chg'))
    cells   = f'<td>{escape(str(r["Instrument"]))}</td>'
    cells  += _cur_price_td(r.get('price_cur'))
    cells  += _price_td(r.get('price_chg'), diverge)
    cells  += _num_td(r['net'])
    cells  += _zbar(r['net_z'])
    cells  += _chg_td(r['net_ww'], r['net_ww_z'])
    cells  += _num_td(r['long'])
    cells  += _zbar(r['long_z'])
    cells  += _chg_td(r['long_ww'], r['long_ww_z'])
    cells  += _num_td(r['short'])
    cells  += _zbar(r['short_z'])
    cells  += _chg_td(r['short_ww'], r['short_ww_z'])
    cells  += _flow_tag(r.get('flow_state', ''))
    cells  += _crowding_tag(r.get('net_z'), r.get('long_z'), r.get('short_z'))
    return f'<tr>{cells}</tr>'


def _build_table_rows(df):
    rows, last_sec = [], None
    for _, r in df.iterrows():
        sec = r.get('_section', '')
        if sec != last_sec:
            last_sec = sec
            rows.append(f'<tr class="sec-row"><td colspan="14">{escape(sec)}</td></tr>')
        rows.append(_row_html(r))
    return ''.join(rows)


def _section_table(df, title, source):
    sub = """<tr class="sub">
        <th>净持仓</th><th>z</th><th>周变化</th>
        <th>多头</th><th>z</th><th>周变化</th>
        <th>空头</th><th>z</th><th>周变化</th>
    </tr>"""
    return f"""
<p class="section-label">{title}</p>
<table>
  <thead>
    <tr>
      <th rowspan="2">资产</th>
      <th rowspan="2" class="gh">当前价格</th>
      <th rowspan="2" class="gh">同期涨跌</th>
      <th colspan="3" class="gh">净持仓</th>
      <th colspan="3" class="gh">多头</th>
      <th colspan="3" class="gh">空头</th>
      <th rowspan="2" class="gh">动作</th>
      <th rowspan="2" class="gh">拥挤度</th>
    </tr>
    {sub}
  </thead>
  <tbody>{_build_table_rows(df)}</tbody>
</table>
<div class="source">数据来源: {source}</div>"""


def _price_detail_table(price_data):
    if not price_data:
        return ''
    rows = []
    for name, info in sorted(price_data.items()):
        cls      = 'pos' if info['ret'] > 0 else ('neg' if info['ret'] < 0 else '')
        decimals = 2 if info['px_start'] >= 1 else 4
        rows.append(
            f'<tr><td>{escape(name)}</td>'
            f'<td style="color:#888">{escape(info["ticker"])}</td>'
            f'<td>{info["date_start"]}</td>'
            f'<td style="text-align:right">{info["px_start"]:,.{decimals}f}</td>'
            f'<td>{info["date_end"]}</td>'
            f'<td style="text-align:right">{info["px_end"]:,.{decimals}f}</td>'
            f'<td class="{cls}" style="text-align:right;font-weight:600">{info["ret"]:+.2f}%</td></tr>'
        )
    return f"""
<div class="detail-wrap">
  <h3>同期涨跌验证明细 (Tue→Tue)</h3>
  <table>
    <thead><tr>
      <th style="text-align:left">资产</th><th>Ticker</th>
      <th>起始日</th><th>起始收盘</th><th>截止日</th><th>截止收盘</th><th>涨跌</th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  <div class="source">yfinance API · 取当日或前最近交易日收盘价</div>
</div>"""


def generate_html(df_tff, df_disagg, report_date, price_data=None):
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CFTC 持仓报告 · {escape(report_date)}</title>
  <style>{CSS}</style>
</head>
<body>
<header>
  <h1>CFTC 期货持仓分析</h1>
  <div class="meta">数据截止 {escape(report_date)}<br>生成 {escape(now)}<br>CFTC Socrata API + yfinance</div>
</header>

{_section_table(df_disagg,'管理资金 Managed Money（COT 分类报告）', 'CFTC Disaggregated COT')}

{_section_table(df_tff,   '杠杆基金 Leveraged Funds（TFF 报告）',   'CFTC Traders in Financial Futures')}
{_price_detail_table(price_data)}
<div class="notes">
  <strong>说明</strong>
  <ul>
    <li>z-score = (当前值 − 156周均值) / 156周标准差（3年窗口）</li>
    <li>周变化 = 周环比合约数变化（括号内为该变化的z-score）</li>
    <li>净持仓 = 多头 − 空头 &nbsp;|&nbsp; 同期涨跌 = CFTC报告期 Tue→Tue 价格变动</li>
    <li>动作: 多头建仓/平仓、空头建仓/回补、多头挤压/空头施压、多空双增/双减</li>
    <li>拥挤度: net/long/short z 任一 ≥ 2.0 → 拥挤 &nbsp;|&nbsp; ≥ 2.75 → 极端</li>
    <li><span style="background:#fff3cd;border:2px solid #ffca2c;padding:1px 5px;font-size:10px;font-weight:700">黄色高亮</span>
        = 同期涨跌背离（看多资金+价格下跌，或看空资金+价格上涨）</li>
    <li>MSCI新兴/发达市场使用 EEM/EFA（ETF 代理†），MSCI 指数本身 yfinance 不可用</li>
  </ul>
</div>


</body>
</html>"""


# ============================================================================
# MAIN
# ============================================================================

def parse_args():
    args = sys.argv[1:]
    target_date = None
    output_file = None
    fetch_price = True

    i = 0
    while i < len(args):
        if args[i] == '--date' and i + 1 < len(args):
            target_date = args[i + 1]; i += 2
        elif args[i] == '--output' and i + 1 < len(args):
            output_file = args[i + 1]; i += 2
        elif args[i] == '--no-price':
            fetch_price = False; i += 1
        else:
            i += 1
    return target_date, output_file, fetch_price


def main():
    target_date, output_file, fetch_price = parse_args()
    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')

    print("=" * 50)
    print("CFTC Positioning Replicator")
    if target_date:
        print(f"  Target date : {target_date}")
    if not fetch_price:
        print("  Price fetch : DISABLED")
    print("=" * 50)

    # 1. Fetch CFTC data
    print("\n[1/4] Fetching CFTC data...")
    print("  TFF (分页拉取)...")
    df_tff = fetch_cftc(CFTC_TFF_URL, start_date)
    print(f"    -> 合计 {len(df_tff)} 行，覆盖 {df_tff['report_date'].nunique()} 个报告期")

    print("  Disaggregated (分页拉取)...")
    df_disagg = fetch_cftc(CFTC_DISAGG_URL, start_date)
    print(f"    -> 合计 {len(df_disagg)} 行，覆盖 {df_disagg['report_date'].nunique()} 个报告期")

    if target_date:
        cutoff    = pd.Timestamp(target_date)
        df_tff    = df_tff[df_tff['report_date'] <= cutoff]
        df_disagg = df_disagg[df_disagg['report_date'] <= cutoff]

    # 以 TFF 和 Disagg 共同最新期的较小值为准，确保两张表同期
    tff_max   = df_tff['report_date'].max()   if not df_tff.empty   else pd.NaT
    disagg_max= df_disagg['report_date'].max() if not df_disagg.empty else pd.NaT
    if pd.isna(tff_max) and pd.isna(disagg_max):
        print("  [ERROR] 两个数据集均为空，退出")
        sys.exit(1)
    report_ts   = min(d for d in [tff_max, disagg_max] if not pd.isna(d))
    report_date = report_ts.strftime('%Y-%m-%d')
    print(f"  TFF  最新期: {tff_max.strftime('%Y-%m-%d') if not pd.isna(tff_max) else 'N/A'}")
    print(f"  Disagg最新期: {disagg_max.strftime('%Y-%m-%d') if not pd.isna(disagg_max) else 'N/A'}")
    print(f"  Report date : {report_date}（取两者较早者，确保数据对齐）")

    # 2. Fetch price data
    price_data = {}
    if fetch_price:
        all_contracts = TFF_CONTRACTS + DISAGG_CONTRACTS
        total_tickers = len([c for c in all_contracts if c.get('yf')])
        print(f"\n[2/4] Fetching price data (Tue→Tue, {MAX_PRICE_WORKERS} workers)...")
        price_data = fetch_tue_tue_returns(all_contracts, report_date)
        print(f"  -> {len(price_data)}/{total_tickers} instruments")
    else:
        print("\n[2/4] Skipping price data (--no-price)")

    # 3. Build tables
    print("\n[3/4] Building tables...")
    df_t12_tff    = build_table12_tff(df_tff, TFF_CONTRACTS, price_data)
    df_t12_disagg = build_table12_disagg(df_disagg, DISAGG_CONTRACTS, price_data)
    print(f"  TFF: {len(df_t12_tff)} instruments | Disagg: {len(df_t12_disagg)} instruments")

    # 4. Write HTML
    print("\n[4/4] Writing HTML...")
    html = generate_html(df_t12_tff, df_t12_disagg, report_date, price_data or None)

    if not output_file:
        output_file = f'cftc_持仓报告_{report_date}.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  -> {output_file}")

    # Preview
    if not df_t12_tff.empty:
        print("\n--- Leveraged Funds Preview ---")
        cols = ['Instrument', 'net', 'net_z', 'net_ww', 'long', 'long_ww', 'short', 'short_ww']
        print(df_t12_tff[cols].head(8).to_string(index=False))


if __name__ == '__main__':
    main()
