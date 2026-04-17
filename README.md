# CFTC 期货持仓分析报告

每周自动拉取 CFTC 持仓数据，生成一份静态 HTML 报告，复现 JPM Delta-One Table 12 风格。

## 报告内容

| 板块 | 说明 |
|------|------|
| 杠杆基金 (TFF) | 股指、债券、利率、外汇/加密 的 Leveraged Funds 持仓 |
| 管理资金 (Disagg) | 能源、金属、农产品 的 Managed Money 持仓 |

每行包含：同期涨跌、净持仓、z-score、周变化、多头、空头、**动作 badge**、**拥挤度 badge**

## 快速开始

### 环境要求

- Python 3.9+
- 网络可访问 `publicreporting.cftc.gov` 和 `finance.yahoo.com`

### 安装依赖

```bash
pip install -r requirements.txt
```

### 运行

```bash
# macOS / Linux — 最新一期
bash run.sh

# Windows — 双击 run.bat，或命令行：
run.bat

# 直接调用脚本
python3 cftc.py

# 指定报告日期（回测历史）
python3 cftc.py --date 2026-03-18

# 自定义输出文件名
python3 cftc.py --output weekly_report.html

# 跳过价格拉取（网络受限时）
python3 cftc.py --no-price
```

生成文件：`cftc_持仓报告_YYYY-MM-DD.html`，直接用浏览器打开即可。

## 每周定时运行

CFTC 每周五（美东时间）发布最新数据，建议设置每周五晚自动运行。

详见 `crontab_example.txt`，其中有完整的 crontab 配置示例和日志说明。

## 报告解读

### 颜色含义

| 颜色 | 含义 |
|------|------|
| 绿色数字 | 正值（多头/净多仓） |
| 红色数字 | 负值（空头/净空仓） |
| 绿色 z-bar | z-score 正值，历史偏高 |
| 红色 z-bar | z-score 负值，历史偏低 |
| 黄色高亮格 | 同期涨跌与持仓动作**背离** |

### 动作 Badge

| Badge | 颜色 | 含义 |
|-------|------|------|
| 多头建仓 | 绿 | 多头显著增加，空头无明显变化 |
| 多头平仓 | 红 | 多头显著减少，空头无明显变化 |
| 空头建仓 | 红 | 空头显著增加，多头无明显变化 |
| 空头回补 | 绿 | 空头显著减少，多头无明显变化 |
| 多头挤压 | 绿 | 多头增加 + 空头减少 |
| 空头施压 | 红 | 空头增加 + 多头减少 |
| 多空双增 | 黄 | 多头空头同时增加 |
| 多空双减 | 黄 | 多头空头同时减少 |

### 拥挤度 Badge

| Badge | 颜色 | 触发条件 |
|-------|------|----------|
| 拥挤多头 | 橙 | net_z 或 long_z ≥ 2.0 |
| 极端多头 | 红 | net_z 或 long_z ≥ 2.75 |
| 拥挤空头 | 橙 | net_z ≤ −2.0 或 short_z ≥ 2.0 |
| 极端空头 | 红 | net_z ≤ −2.75 或 short_z ≥ 2.75 |

### z-score 说明

```
z = (当前值 − 156周均值) / 156周标准差
```

- 窗口：3年（156周）
- z-score 基于 OI 归一化后的持仓占比计算
- |z| > 2 为统计显著偏离

## 数据来源

| 数据 | 来源 | 说明 |
|------|------|------|
| CFTC TFF 持仓 | [CFTC Socrata API](https://publicreporting.cftc.gov/resource/gpe5-46if.json) | 免费，无需 API Key |
| CFTC Disagg 持仓 | [CFTC Socrata API](https://publicreporting.cftc.gov/resource/72hh-3qpy.json) | 免费，无需 API Key |
| 同期价格 | yfinance | Tue→Tue 收盘价变动 |

## 合约映射

### TFF（Leveraged Funds）

| 资产 | CFTC 名称 | Yahoo Finance |
|------|-----------|---------------|
| 标普500 | E-MINI S&P 500 - | ^GSPC |
| 纳斯达克100 | NASDAQ MINI | ^NDX |
| 罗素2000 | RUSSELL E-MINI | ^RUT |
| MSCI新兴市场 | MSCI EM INDEX | EEM† |
| MSCI发达市场 | MSCI EAFE | EFA† |
| 日经225 | NIKKEI STOCK AVERAGE | ^N225 |
| 2年期美债 | UST 2Y NOTE | ZT=F |
| 10年期美债 | UST 10Y NOTE | ZN=F |
| 超长期美债 | ULTRA UST BOND | UB=F |
| 联邦基金 | FED FUNDS | ZQ=F |
| 欧元/美元 | EURO FX - CHICAGO | EURUSD=X |
| 英镑/美元 | BRITISH POUND | GBPUSD=X |
| 日元/美元 | JAPANESE YEN | JPYUSD=X |
| 澳元/美元 | AUSTRALIAN DOLLAR | AUDUSD=X |
| 比特币 | BITCOIN - CHICAGO MERCANTILE | BTC-USD |

† MSCI 指数本身在 yfinance 不可用，使用 ETF 代理

### Disagg（Managed Money）

| 资产 | CFTC 名称 | Yahoo Finance |
|------|-----------|---------------|
| WTI原油 | WTI-PHYSICAL | CL=F |
| 天然气 | NAT GAS NYME | NG=F |
| 铜 | COPPER- #1 | HG=F |
| 黄金 | GOLD - COMMODITY | GC=F |
| 白银 | SILVER - COMMODITY | SI=F |
| 玉米 | CORN - CHICAGO | ZC=F |

## 网络重试策略

CFTC API 网络不稳定时，脚本自动重试（指数退避）：

| 第几次失败 | 等待时间 |
|-----------|----------|
| 第 1 次 | 1 秒 |
| 第 2 次 | 2 秒 |
| 第 3 次 | 4 秒 |
| 第 4 次 | 8 秒 |
| 第 5 次 | 放弃并报错 |

网络极差时，可用 `--no-price` 跳过 yfinance，只拉 CFTC 数据。

## 文件说明

```
cftc_project/
├── cftc.py              # 主脚本
├── requirements.txt     # Python 依赖
├── run.sh               # macOS/Linux 一键运行
├── run.bat              # Windows 一键运行
├── crontab_example.txt  # 定时任务配置示例
└── README.md            # 本文件
```
