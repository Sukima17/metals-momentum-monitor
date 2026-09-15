# 金属多周期动量工作台

面向期货基本面研究员的技术信号辅助工具。日线四因子是主模型，周线与 60 分钟线用于趋势确认，5 分钟线只用于盘中预警。覆盖沪铜、沪铝、沪铅、沪锌、沪镍、碳酸锂、沪锡、黄金、白银，并为 LME 钴保留外部授权数据适配层。

## 频率与模型

- 日线四因子：5 日收益率超过 ±3%、突破 20 日高低、收盘相对 MA20、日线 RSI14。
- 触发规则：日线至少 3/4 因子同向，再叠加周线和 60 分钟趋势形成综合结论。
- 盘中预警：5 分钟线每 5 分钟巡检；不会替代日线主信号。
- 收盘确认：原始接口返回完整留档，指标层剔除未完成 K 线。
- 快速回测：信号后下一交易日开盘入场，最长持有 10 个交易日，包含双边成本与 2 跳滑点。

## 本地使用

```powershell
.\start-dashboard.ps1
```

打开 `http://127.0.0.1:8765`。该窗口保持运行时，页面会每 5 分钟自动扫描，“立即扫描”按钮也可使用。

只扫描一次：

```powershell
.\run-once.ps1
```

安装 Windows 定时任务：

```powershell
.\install-scheduled-task.ps1
```

## 手机访问

GitHub Pages：<https://sukima17.github.io/metals-momentum-monitor/>

云端版本在中国交易时段约每 15 分钟刷新一次。GitHub Actions 定时任务可能有几分钟排队延迟；页面会显示真实数据时间和日线截止日期。

## 钴数据

LME 钴不使用国内现货或模拟值替代。需要分别提供：

- `data/cobalt_daily.csv`：至少 60 根日线；
- `data/cobalt_60m.csv`：至少 35 根 60 分钟线；
- `data/cobalt_5m.csv`：至少 35 根 5 分钟线。

字段为：

```text
datetime,open,high,low,close,volume,hold,settle
```

## 数据边界

国内行情来自新浪财经日线及分钟线接口（与 AKShare `futures_zh_minute_sina` / `futures_zh_daily_sina` 同源）。公开源适合研究原型，不建议直接驱动交易。生产使用应替换为 Wind、iFinD、CTP 或交易所授权源，并使用后复权连续合约或固定主力合约处理换月跳空。

`position_changes` 是主连持仓量变化，不等同于资金净流入；换月附近尤其需要谨慎解释。

本工具仅用于研究，不构成投资建议或自动交易指令。
