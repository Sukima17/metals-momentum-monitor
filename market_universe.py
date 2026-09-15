"""Sina main-continuous domestic futures breadth snapshot."""

from __future__ import annotations

import re
import urllib.request
from datetime import datetime
from typing import Any


UNIVERSE = [
    # CZCE
    ("TA0","PTA","CZCE","energy"),("OI0","菜油","CZCE","agriculture"),("RS0","菜籽","CZCE","agriculture"),("RM0","菜粕","CZCE","agriculture"),("ZC0","动力煤","CZCE","energy"),("WH0","强麦","CZCE","agriculture"),("JR0","粳稻","CZCE","agriculture"),("SR0","白糖","CZCE","agriculture"),("CF0","棉花","CZCE","agriculture"),("RI0","早籼稻","CZCE","agriculture"),("MA0","甲醇","CZCE","energy"),("FG0","玻璃","CZCE","energy"),("LR0","晚籼稻","CZCE","agriculture"),("SF0","硅铁","CZCE","ferrous"),("SM0","锰硅","CZCE","ferrous"),("CY0","棉纱","CZCE","agriculture"),("AP0","苹果","CZCE","agriculture"),("CJ0","红枣","CZCE","agriculture"),("UR0","尿素","CZCE","energy"),("SA0","纯碱","CZCE","energy"),("PF0","短纤","CZCE","energy"),("PK0","花生","CZCE","agriculture"),("SH0","烧碱","CZCE","energy"),("PX0","对二甲苯","CZCE","energy"),("PR0","瓶片","CZCE","energy"),("PL0","丙烯","CZCE","energy"),
    # DCE
    ("V0","PVC","DCE","energy"),("P0","棕榈油","DCE","agriculture"),("B0","豆二","DCE","agriculture"),("M0","豆粕","DCE","agriculture"),("I0","铁矿石","DCE","ferrous"),("JD0","鸡蛋","DCE","agriculture"),("L0","塑料","DCE","energy"),("PP0","聚丙烯","DCE","energy"),("FB0","纤维板","DCE","agriculture"),("BB0","胶合板","DCE","agriculture"),("Y0","豆油","DCE","agriculture"),("C0","玉米","DCE","agriculture"),("A0","豆一","DCE","agriculture"),("J0","焦炭","DCE","ferrous"),("JM0","焦煤","DCE","ferrous"),("CS0","玉米淀粉","DCE","agriculture"),("EG0","乙二醇","DCE","energy"),("RR0","粳米","DCE","agriculture"),("EB0","苯乙烯","DCE","energy"),("PG0","液化气","DCE","energy"),("LH0","生猪","DCE","agriculture"),("LG0","原木","DCE","agriculture"),("BZ0","纯苯","DCE","energy"),
    # SHFE + INE
    ("FU0","燃油","SHFE/INE","energy"),("SC0","原油","SHFE/INE","energy"),("AL0","沪铝","SHFE","nonferrous"),("RU0","橡胶","SHFE","energy"),("ZN0","沪锌","SHFE","nonferrous"),("CU0","沪铜","SHFE","nonferrous"),("AU0","黄金","SHFE","precious"),("RB0","螺纹钢","SHFE","ferrous"),("WR0","线材","SHFE","ferrous"),("PB0","沪铅","SHFE","nonferrous"),("AG0","白银","SHFE","precious"),("BU0","沥青","SHFE","energy"),("HC0","热卷","SHFE","ferrous"),("SN0","沪锡","SHFE","nonferrous"),("NI0","沪镍","SHFE","nonferrous"),("SP0","纸浆","SHFE","energy"),("NR0","20号胶","SHFE/INE","energy"),("SS0","不锈钢","SHFE","ferrous"),("LU0","低硫燃油","SHFE/INE","energy"),("BC0","国际铜","SHFE/INE","nonferrous"),("AO0","氧化铝","SHFE","nonferrous"),("BR0","丁二烯橡胶","SHFE","energy"),("EC0","集运欧线","SHFE/INE","energy"),("AD0","铸造铝合金","SHFE","nonferrous"),("OP0","胶版印刷纸","SHFE","energy"),
    # CFFEX
    ("IF0","沪深300股指","CFFEX","financial"),("TF0","5年国债","CFFEX","financial"),("T0","10年国债","CFFEX","financial"),("IH0","上证50股指","CFFEX","financial"),("IC0","中证500股指","CFFEX","financial"),("TS0","2年国债","CFFEX","financial"),("IM0","中证1000股指","CFFEX","financial"),
    # GFEX
    ("SI0","工业硅","GFEX","new_energy"),("LC0","碳酸锂","GFEX","new_energy"),("PS0","多晶硅","GFEX","new_energy"),("PT0","铂","GFEX","precious"),("PD0","钯","GFEX","precious"),
]


def _request(symbols: list[str]) -> str:
    url = "https://hq.sinajs.cn/list=" + ",".join("nf_" + symbol for symbol in symbols)
    request = urllib.request.Request(url, headers={"Referer": "https://vip.stock.finance.sina.com.cn/", "User-Agent": "Mozilla/5.0 MarketBreadth/1.0"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return response.read().decode("gb18030", errors="replace")


def fetch_market_breadth(generated_at: datetime) -> dict[str, Any]:
    raw = "".join(_request([item[0] for item in UNIVERSE[index : index + 40]]) for index in range(0, len(UNIVERSE), 40))
    values = {symbol: payload.split(",") for symbol, payload in re.findall(r'var hq_str_nf_([A-Za-z0-9]+)="([^"]*)"', raw)}
    rows = []
    for symbol, name, exchange, category in UNIVERSE:
        fields = values.get(symbol)
        if not fields:
            continue
        try:
            if exchange == "CFFEX":
                current, previous, volume, hold = float(fields[3]), float(fields[26]), float(fields[4]), float(fields[6])
                date, quote_time = fields[37], fields[38]
            else:
                current, previous, volume, hold = float(fields[8]), float(fields[10]), float(fields[14]), float(fields[13])
                date, quote_time = fields[17], fields[1]
            if current <= 0 or previous <= 0:
                continue
            rows.append({"symbol": symbol, "name": name, "exchange": exchange, "category": category, "price": current, "previous_settlement": previous, "change_pct": (current / previous - 1) * 100, "volume": volume, "hold": hold, "quote_time": f"{date} {quote_time}"})
        except (IndexError, TypeError, ValueError):
            continue
    rows.sort(key=lambda row: row["change_pct"], reverse=True)
    category_keys = ("precious", "nonferrous", "ferrous", "energy", "agriculture", "new_energy", "financial")
    categories = {key: {"up": sum(row["change_pct"] > 0.005 for row in rows if row["category"] == key), "down": sum(row["change_pct"] < -0.005 for row in rows if row["category"] == key), "flat": sum(abs(row["change_pct"]) <= 0.005 for row in rows if row["category"] == key), "valid": sum(row["category"] == key for row in rows)} for key in category_keys}
    return {
        "generated_at": generated_at.isoformat(timespec="seconds"), "source": "新浪财经国内期货实时行情（主力连续，每品种一条）",
        "scope": "SHFE/INE、DCE、CZCE、GFEX、CFFEX；覆盖商品、新能源与金融期货",
        "universe": len(UNIVERSE), "valid": len(rows), "up": sum(row["change_pct"] > 0.005 for row in rows), "down": sum(row["change_pct"] < -0.005 for row in rows), "flat": sum(abs(row["change_pct"]) <= 0.005 for row in rows),
        "top_gainers": rows[:3], "top_losers": list(reversed(rows[-3:])), "categories": categories,
        "metal_details": [row for row in rows if row["category"] in ("precious", "nonferrous", "ferrous")],
        "last_quote": max((row["quote_time"] for row in rows), default=None),
    }

